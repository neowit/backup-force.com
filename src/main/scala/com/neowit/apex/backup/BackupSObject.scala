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
import com.sforce.ws.ConnectionException

object ZuluTime {
    val zulu = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    zulu.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))
    def format(d: java.util.Date):String = zulu.format(d)

}

class BatchProcessingException(msg:String, code: AsyncExceptionCode) extends AsyncApiException(msg: String, code: AsyncExceptionCode ) {
    def this(msg: String) = this(msg, AsyncExceptionCode.InvalidBatch)
}
class BackupSObject(connection:PartnerConnection, objectApiName:String ) {

    val appConfig = Config.getConfig

    def run(): Future[(OperationMode, Boolean)] = {
        require(null != appConfig.outputFolder, "config file missing 'outputFolder' value")
        val describeRes = connection.describeSObject(objectApiName)
        val allFields = describeRes.getFields

        val outputFilePath = appConfig.outputFolder + File.separator + objectApiName + ".csv"

        val configSoql = appConfig.getProperty("backup.soql." + objectApiName)

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
                    "select * from " + objectApiName + {if (currentMode.allowGlobalWhere) " where " + appConfig.globalWhere.get else ""}


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


                try {
                    //get current server time, i.e. time at the time when query started
                    val timeStampCal = connection.getServerTimestamp.getTimestamp

                    println(objectApiName + ": Trying mode: " + currentMode)
                    val size = currentMode.load(connection, objectApiName, soqlParser, queryString, fieldList, outputFilePath)

                    onFileComplete(objectApiName, outputFilePath, size)
                    println(objectApiName + ": " + size + " " + {if (currentMode.isAsync) "bytes" else "lines"})
                    result = (currentMode, true)
                    //store date/time in lastQuery.properties
                    appConfig.storeLastModifiedDate(objectApiName, ZuluTime.format(timeStampCal.getTime))
                    //appConfig.lastQueryPropsActor ! ("store", objectApiName, ZuluTime.format(timeStampCal.getTime))

                } catch {
                    case ex: InvalidFieldFault => println(ex); if (currentMode.allowGlobalWhere) println("Will try once again without global.where")
                    case ex: ApiQueryFault =>
                        if (ex.getExceptionMessage.indexOf("Implementation restriction:") >=0) {
                            println("Warning: Object " + objectApiName +" can not be queried in batch mode due to Implementation restriction")
                            println("\t" + ex.getExceptionMessage)
                        } else {
                            //all other ApiQueryFault related problems
                            println("Object " + objectApiName +" retrieve failed")
                            println(ex)
                        }
                    case ex: BatchProcessingException => println(ex.getExceptionMessage)
                    case ex: AsyncApiException =>
                        ex.getExceptionCode match {
                            case AsyncExceptionCode.InvalidEntity =>
                                //exceptionMessage='Entity 'xxx' is not supported by the Bulk API.'
                                println("Warning: Object " + objectApiName +" can not be queried in Bulk API mode ")
                                println("\t" + ex)
                            case AsyncExceptionCode.ExceededQuota =>
                                println("Warning: Object " + objectApiName +" can not be queried in Bulk API mode ")
                                println("\t" + ex)
                                println("\tSwitching to standard Web Service API mode...\n")
                                appConfig.stopBulkApi()
                            case _ =>
                                println("Object " + objectApiName +" - Bulk API retrieve failed.")
                                println("\t" + ex)
                                println(ex.getStackTraceString)
                        }
                    case ex: OutOfMemoryError =>
                        println("Error: Object " + objectApiName +" retrieve failed - OutOfMemoryError")
                        println("\tSometimes this error can be avoided by changing java command line parameters")
                        println("\tFor example, to allow java machine to use 1024MB RAM add -Xmx parameter to your command line like so:")
                        println("\tjava -Xmx1024m -jar …\n")

                        println(ex)
                        println(ex.getStackTraceString)

                    case ex: Throwable =>
                        println("Object " + objectApiName +" retrieve failed")
                        if (ex.isInstanceOf[ConnectionException] && ex.getMessage.indexOf("Failed to send request to") >=0) {
                            println("Error: Communication problem. Try to increase value of 'http.connectionTimeoutSecs' and/or 'http.readTimeoutSecs' configuration parameters.\n")
                        }
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
        if (appConfig.useBulkApi) {
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
            appConfig.HookEachAfter.execute(objectApiName, outputFilePath, size)
        }
    }

}

