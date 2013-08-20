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
import com.sforce.soap.partner.fault.{ApiQueryFault, InvalidFieldFault}
import com.sforce.async._
import java.io.File

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

object ZuluTime {
    val zulu = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    zulu.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))
    def format(d: java.util.Date):String = zulu.format(d)

}

class BatchProcessingException(msg:String, code: AsyncExceptionCode) extends AsyncApiException(msg: String, code: AsyncExceptionCode ) {
    def this(msg: String) = this(msg, AsyncExceptionCode.InvalidBatch)
}
class BackupSObject(connection:PartnerConnection, objectApiName:String ) {


    def run(): Future[(OperationMode, Boolean)] = {
        require(null != Config.outputFolder, "config file missing 'outputFolder' value")
        val describeRes = connection.describeSObject(objectApiName)
        val allFields = describeRes.getFields

        val outputFilePath = Config.outputFolder + File.separator + objectApiName + ".csv"

        val configSoql = Config.getProperty("backup.soql." + objectApiName)

        val modes = List(AsyncWithGlobalWhere, AsyncWithoutGlobalWhere, SyncWithGlobalWhere, SyncWithoutGlobalWhere)
                        .filter(m => m.isApplicable(objectApiName))


        /**
         * try each mode in sequence
         * @return - (Mode, success=true/false)
         */
        def runOneMode(availableModes: List[OperationMode]): (OperationMode, Boolean) = {
            val currentMode = availableModes.head

            val soql =
                if (None != configSoql)
                    configSoql.get
                else
                    "select * from " + objectApiName + {if (currentMode.allowGlobalWhere) " where " + Config.globalWhere.get else ""}


            val soqlParser = new SOQLParser(soql)

            val fieldList = if (soqlParser.isAllFields)
                allFields.filter(!_.isCalculated).map(f => f.getName).toList
            else
                soqlParser.fields

            val fieldSet = fieldList.map(_.toLowerCase).toSet[String]
            val fieldDefs = allFields.filter(f => fieldSet.contains(f.getName.toLowerCase))

            var result = (currentMode, false)

            if (currentMode.isReallyApplicable(soqlParser, fieldDefs)) {
                //fields string entered by user and to be queried
                val selectFieldsStr = if (soqlParser.isAllFields)
                    fieldList.mkString(",")
                else soqlParser.select

                val queryString = {"select " + selectFieldsStr + " from " + objectApiName +
                    (if (soqlParser.hasTail) " " + soqlParser.tail else "") }

                //get current server time, i.e. time at the time when query started
                val timeStampCal = connection.getServerTimestamp.getTimestamp

                try {
                    println(objectApiName + ": Trying mode: " + currentMode)
                    val size = currentMode.load(connection, objectApiName, soqlParser, queryString, fieldList, outputFilePath)

                    onFileComplete(objectApiName, outputFilePath, size)
                    println(objectApiName + ": " + size + " " + {if (currentMode.isAsync) "bytes" else "lines"})
                    result = (currentMode, true)
                    //store date/time in lastQuery.properties
                    //Config.storeLastModifiedDate(objectApiName, ZuluTime.format(timeStampCal.getTime))
                    Config.lastQueryPropsActor ! ("store", objectApiName, ZuluTime.format(timeStampCal.getTime))

                } catch {
                    case ex: InvalidFieldFault => println(ex); if (currentMode.allowGlobalWhere) println("Will try once again without global.where")
                    case ex: ApiQueryFault if ex.getExceptionMessage.indexOf("Implementation restriction:") >=0  =>
                        println("Object " + objectApiName +" can not be queried in batch mode due to Implementation restriction")
                        println(ex)
                        println(ex.getStackTraceString)
                    case ex: BatchProcessingException => println(ex.getExceptionMessage)
                    case ex:Throwable =>
                        println("Object " + objectApiName +" retrieve failed")
                        println(ex)
                        println(ex.getStackTraceString)
                }
            }

            if (!result._2 && !availableModes.tail.isEmpty)
                runOneMode(availableModes.tail)
            else
                result
        }

        //finally - run the process
        if (Config.useBulkApi) {
            val f = future {
                runOneMode(modes)
            }
            f
        } else {
            //sync mode, return nothing
            runOneMode(modes)
            null
        }

    }

    def onFileComplete(objectApiName: String, outputFilePath: String, size: Long) {
        if (size> 0) {
            Config.HookEachAfter.execute(objectApiName, outputFilePath, size)
        }
    }

}

