/*
 * Copyright (c) 2013 Andrey Gavrikov.
 * this file is part of Backup-force.com application
 * https://github.com/neowit/backup-force.com
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.neowit.apex.backup

import com.sforce.soap.partner.PartnerConnection
import java.io.{FileOutputStream, File, FileWriter}
import com.sforce.soap.partner.fault.{ApiQueryFault, InvalidFieldFault}
import com.sforce.soap.partner.sobject.SObject
import com.sforce.ws.util.Base64


class BackupSObject(connection:PartnerConnection, objectApiName:String ) {
    val ALLOW_GLOBAL_WHERE = true
    val DISABLE_GLOBAL_WHERE = !ALLOW_GLOBAL_WHERE

    def run() {
        if (!run(ALLOW_GLOBAL_WHERE)) {
            //if query with globalWhere fails, try once again without it
            run(DISABLE_GLOBAL_WHERE)
        }
    }
    def run(allowGlobalWhere: Boolean): Boolean = {
        val configSoql = Config.getProperty("backup.soql." + objectApiName)
        val soql =
            if (null != configSoql)
                configSoql
            else
                "select * from " + objectApiName +
                    {if (allowGlobalWhere && null != Config.globalWhere) " where " + Config.globalWhere
                    else ""}


        val soqlParser = new SOQLParser(soql)

        val describeRes = connection.describeSObject(objectApiName)
        val allFields = describeRes.getFields
        val fieldList = if (soqlParser.isAllFields) {
            allFields.filter(!_.isCalculated).map(f => f.getName).toList
        } else {
            //fix case of all field names - user defined fields are not always correctly formatted
            // and as a result record.getField(fName) may return nothing
            val selectedFieldsSet = soqlParser.fields.map(_.toLowerCase).toSet
            allFields.filter(f => selectedFieldsSet.contains(f.getName.toLowerCase)).map(f => f.getName).toList
        }

        require(null != Config.outputFolder, "config file missing 'outputFolder' value")

        val queryString = {"select " + fieldList.mkString(",") + " from " + objectApiName +
                            (if (soqlParser.hasTail) " " + soqlParser.tail else "") }

        val outputFilePath = Config.outputFolder + File.separator + objectApiName + ".csv"
        var writerNeedsClosing = false
        lazy val csvWriter = {
            println("about to start:  " + outputFilePath)
            Config.HookEachBefore.execute(objectApiName, outputFilePath)
            val file = new File(outputFilePath)
            file.createNewFile()
            val writer = new FileWriter(file)
            writerNeedsClosing = true
            new com.sforce.bulk.CsvWriter(fieldList.toArray[String], writer)
        }

        var result = false
        val timeStampCal = connection.getServerTimestamp.getTimestamp
        val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")

        try {
            var queryResults = connection.query(queryString)
            val size = queryResults.getSize
            if (size > 0) {
                var doExit = false
                do {
                    for (record <- queryResults.getRecords) {
                        //println("Id: " + record.getId + "; Name=" + record.getField("Name"))
                        val values = fieldList.map(fName => (record.getField(fName) match {
                            case null => ""
                            case x => x
                        }).toString).toArray
                        csvWriter.writeRecord(values)
                        processRecord(record)
                    }
                    doExit = queryResults.isDone
                    if (!doExit ){
                        queryResults = connection.queryMore(queryResults.getQueryLocator)
                    }
                } while (!doExit)
            }
            if (writerNeedsClosing) {
                csvWriter.endDocument()
                Config.HookEachAfter.execute(objectApiName, outputFilePath, size)
            }
            println(objectApiName + ": " + size)
            result = true
            //store date/time in lastQuery.properties
            Config.storeLastModifiedDate(objectApiName, format.format(timeStampCal.getTime))

        } catch {
            case ex: InvalidFieldFault => println(ex); if (allowGlobalWhere) println("Will try once again without global.where")
            case ex: ApiQueryFault if ex.getExceptionMessage.indexOf("Implementation restriction:") >=0  =>
                println("Object " + objectApiName +" can not be queried in batch mode due to Implementation restriction")
                println(ex)
                println(ex.getStackTraceString)
            case ex:Throwable =>
                println("Object " + objectApiName +" retrieve failed")
                println(ex)
                println(ex.getStackTraceString)
        }

        result
    }

    private def processRecord(record: SObject) {
        //save attachment as file if "backup.attachment.asfile" is not null
        val FILE_OBJ_TYPES = Map("attachment" -> ("Name", "Body"), "document" -> ("Name", "Body"),
                                 "contentversion" -> ("PathOnClient", "VersionData"))
        val objectApiName = record.getType.toLowerCase
        if ( FILE_OBJ_TYPES.contains(objectApiName)) {
            val fileNameField = FILE_OBJ_TYPES(objectApiName)._1
            val fileBodyField = FILE_OBJ_TYPES(objectApiName)._2

            val fileName = Config.getProperty("backup.extract.file") match {
                case str:String if str.length >0 =>
                    val fileName = record.getField(fileNameField) match {
                        case null => ""
                        case x => x.toString
                    }
                    val extIndex1 = fileName.lastIndexOf(".")
                    val extIndex = if (extIndex1 >= 0) extIndex1 else fileName.length
                    val name = fileName.substring(0, extIndex)
                    val ext = if (extIndex < fileName.length) fileName.substring(extIndex+1) else ""
                    str.replaceAll("""$name""", name).replaceAll("""$id""", record.getId).replaceAll("""$ext""", ext)
                case _ => null
            }

            if (null != fileName) {
                val file = new File(Config.mkdirs(record.getType) + File.separator + fileName)
                val buffer = record.getField(fileBodyField) match {
                    case null => Array[Byte]()
                    case x => x.toString.getBytes()
                }
                if (buffer.length > 0) {
                    val output = new FileOutputStream(file)
                    output.write(Base64.decode(buffer))
                    output.close()
                }
            }
        }
    }

}
