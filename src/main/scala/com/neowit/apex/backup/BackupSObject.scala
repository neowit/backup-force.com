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
import java.io.{File, FileWriter}
import com.sforce.soap.partner.fault.{MalformedQueryFault, InvalidFieldFault}


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
                configSoql.toLowerCase
            else
                "select * from " + objectApiName +
                    {if (allowGlobalWhere && null != Config.globalWhere) " where " + Config.globalWhere
                    else ""}


        val soqlParser = new SOQLParser(soql)

        val describeRes = connection.describeSObject(objectApiName)
        val fieldList = if (soqlParser.isAllFields) {
            describeRes.getFields.filter(!_.isCalculated).map(f => f.getName).toList
        } else {
            soqlParser.fields.map(fName => fName.substring(0, 1).toUpperCase + fName.substring(1))
        }
        val whereCondStr = soqlParser.where
        require(null != Config.outputFolder, "config file missing 'outputFolder' value")

        val queryString = {"select " + fieldList.mkString(",") + " from " + objectApiName +
            (if (null != whereCondStr) " where " + whereCondStr else "") +
            (if (soqlParser.hasLimit) " limit " + soqlParser.limit  else "")}

        val file = new File(Config.outputFolder + File.separator + objectApiName + ".csv")
        file.createNewFile()
        val writer = new FileWriter(file)
        val csvWriter = new com.sforce.bulk.CsvWriter(fieldList.toArray[String], writer)

        var result = false
        try {
            val queryResults = connection.query(queryString)
            val size = queryResults.getSize
            if (size > 0) {
                for (record <- queryResults.getRecords) {
                    //println("Id: " + record.getId + "; Name=" + record.getField("Name"))
                    val values = fieldList.map(fName => (record.getField(fName) match {
                        case null => ""
                        case x => x
                    }).toString).toArray
                    csvWriter.writeRecord(values)
                }
            }
            println(objectApiName + ": " + size)
            result = true
        } catch {
            case ex: InvalidFieldFault => println(ex); if (allowGlobalWhere) println("Will try once again without global.where")
            case ex: MalformedQueryFault if ex.getExceptionMessage.indexOf("Implementation restriction:") >=0  =>
                println(ex); println("Object " + objectApiName +" can not be queried due to Implementation restriction")
        }
        csvWriter.endDocument()

        result
    }
}
