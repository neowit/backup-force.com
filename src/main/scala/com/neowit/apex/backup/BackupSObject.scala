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

import com.sforce.soap.partner.{DescribeSObjectResult, PartnerConnection}
import java.io.{FileOutputStream, File, FileWriter}
import com.sforce.soap.partner.fault.{ApiQueryFault, InvalidFieldFault}
import com.sforce.soap.partner.sobject.SObject
import com.sforce.ws.util.Base64
import com.sforce.ws.bind.XmlObject

object ZuluTime {
    val zulu = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    zulu.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))
    def format(d: java.util.Date):String = zulu.format(d)

}

/**
 * this class serves two purposes
 * 1 - resolves field names typed in mixed case and maps them to the properly formatted field names
 *      which XmlObject.getField() method expects, e.g. agents_name__c -> Agents_Name__c
 * 2 - resolves relationships, e.g. Owner.Name
 */
class FieldResolver (describeResult: DescribeSObjectResult) {

    val fieldNameByLowerCaseName = describeResult.getFields.map(f => (f.getName.toLowerCase, f.getName)).toMap

    def getField(record: XmlObject, fName: String): Object = {
        val childByLowerCaseKey = getFieldMapFromChildrenNames(Map[String, String](), record.getChildren)

        if (fName.indexOf(".") < 1) {
            //normal field
            val realName = fieldNameByLowerCaseName.get(fName.toLowerCase) match {
                case Some(x) => x
                case None => //could not find this field on current object, chances that it is part of the relationship

                    childByLowerCaseKey.get(fName.toLowerCase) match {
                        case Some(x) => x
                        case None => fName //fall back to the original name
                    }
            }
            record.getField(realName)
        } else {
            //relationship field
            //Account.Agents_Name__r.Name
            val head = fName.takeWhile(_ != '.') //Account
            val properName = childByLowerCaseKey.get(head.toLowerCase) match {
                case Some(x) => x
                case None => head //fall back to the original name
            }
            val tail = fName.substring(properName.length + 1) //Agents_Name__r.Name
            getField(record.getChild(properName), tail)
        }
    }

    /**
     * iterate through all children and map lower case names to real field names
     */
    def getFieldMapFromChildrenNames(fMap: Map[String, String], children: java.util.Iterator[XmlObject]): Map[String, String] = {
        if (children.hasNext) {
            val child = children.next()
            val fName = child.getName.getLocalPart
            getFieldMapFromChildrenNames(fMap ++ Map(fName.toLowerCase -> fName), children )
        } else fMap
    }
}

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
        require(null != Config.outputFolder, "config file missing 'outputFolder' value")

        val configSoql = Config.getProperty("backup.soql." + objectApiName)
        val soql =
            if (None != configSoql)
                configSoql.get
            else
                "select * from " + objectApiName +
                    {if (allowGlobalWhere && None != Config.globalWhere) " where " + Config.globalWhere.get
                    else ""}


        val soqlParser = new SOQLParser(soql)


        val describeRes = connection.describeSObject(objectApiName)
        val allFields = describeRes.getFields

        val fieldList = if (soqlParser.isAllFields)
                                allFields.filter(!_.isCalculated).map(f => f.getName).toList
                           else
                                soqlParser.fields

        //fields string entered by user and to be queried
        val selectFieldsStr = if (soqlParser.isAllFields)
                                fieldList.mkString(",")
                              else soqlParser.select

        val queryString = {"select " + selectFieldsStr + " from " + objectApiName +
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
        //get current server time, i.e. time at the time when query started
        val timeStampCal = connection.getServerTimestamp.getTimestamp

        try {
            var queryResults = connection.query(queryString)
            val size = queryResults.getSize
            if (size > 0) {
                val resolver = new FieldResolver(describeRes)
                var doExit = false
                do {
                    for (record <- queryResults.getRecords) {
                        //println("Id: " + record.getId + "; Name=" + record.getField("Name"))
                        val values = fieldList.map(fName => (resolver.getField(record, fName) match {
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
            Config.storeLastModifiedDate(objectApiName, ZuluTime.format(timeStampCal.getTime))

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
                case Some(str) if str.length >0 =>
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
