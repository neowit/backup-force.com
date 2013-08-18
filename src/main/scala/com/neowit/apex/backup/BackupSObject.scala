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
import java.io.{ByteArrayInputStream, FileOutputStream, File, FileWriter}
import com.sforce.soap.partner.fault.{ApiQueryFault, InvalidFieldFault}
import com.sforce.soap.partner.sobject.SObject
import com.sforce.ws.util.Base64
import com.sforce.ws.bind.XmlObject
import scala.collection.JavaConversions._
import scala.annotation.tailrec
import com.sforce.async._

object ZuluTime {
    val zulu = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    zulu.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))
    def format(d: java.util.Date):String = zulu.format(d)

}

class BatchProcessingException(msg:String, code: AsyncExceptionCode) extends AsyncApiException(msg: String, code: AsyncExceptionCode ) {
    def this(msg: String) = this(msg, AsyncExceptionCode.InvalidBatch)
}
/**
 * this class serves two purposes
 * 1 - resolves field names typed in mixed case and maps them to the properly formatted field names
 *      which XmlObject.getField() method expects, e.g. agents_name__c -> Agents_Name__c
 * 2 - resolves relationships, e.g. Owner.Name
 */
class FieldResolver (rec: SObject) {
    private def findChild(localName: String, record: XmlObject): Option[XmlObject] = {
        @tailrec
        def findFirst(it: Iterator[XmlObject]): Option[XmlObject] = {
            if (it.hasNext) {
                val child = it.next()
                if (localName.equalsIgnoreCase(child.getName.getLocalPart)) {
                    Option(child)
                } else {
                    findFirst(it)
                }
            } else
                None
        }
        findFirst(record.getChildren)
    }

    def getFieldIgnoreCase(name: String): Object = {
        getField(name, rec)
    }
    @tailrec
    private def getField(name: String, record: XmlObject): Object = {
        val fName = name.takeWhile(_ != '.')
        findChild(fName, record)  match {
          case Some(item) =>
              if (item.hasChildren) {
                  getField(name.substring(fName.length+1), item)
              } else {
                  item.getValue
              }
          case None => null
        }
    }
}

class BackupSObject(connection:PartnerConnection, objectApiName:String ) {
    //provide conversion of SObject to FieldResolver
    implicit def toFieldResolver(record: SObject) = new FieldResolver(record)

    sealed abstract class Mode
    case object AsyncWithGlobalWhere extends Mode
    case object AsyncWithoutGlobalWhere extends Mode
    case object SyncWithGlobalWhere extends Mode
    case object SyncWithoutGlobalWhere extends Mode

    def run() {
        require(null != Config.outputFolder, "config file missing 'outputFolder' value")
        val describeRes = connection.describeSObject(objectApiName)
        val allFields = describeRes.getFields

        val outputFilePath = Config.outputFolder + File.separator + objectApiName + ".csv"

        val allAsyncModes = Set(AsyncWithGlobalWhere, AsyncWithoutGlobalWhere)

        /**
         * try each mode in sequecnce
         * @return - (Mode, success=true/false)
         */
        def runOneMode(availableModes: List[Mode]): (Mode, Boolean) = {
            val currentMode = availableModes.head

            val allowGlobalWhere = Set(AsyncWithGlobalWhere, SyncWithoutGlobalWhere).contains(currentMode)
            val configSoql = Config.getProperty("backup.soql." + objectApiName)
            val soql =
                if (None != configSoql)
                    configSoql.get
                else
                    "select * from " + objectApiName +
                        {if (allowGlobalWhere && None != Config.globalWhere) " where " + Config.globalWhere.get
                        else ""}


            val soqlParser = new SOQLParser(soql)

            val fieldList = if (soqlParser.isAllFields)
                allFields.filter(!_.isCalculated).map(f => f.getName).toList
            else
                soqlParser.fields

            val fieldSet = fieldList.map(_.toLowerCase).toSet[String]
            val hasBase64Fields = allFields.filter(f => f.getType == com.sforce.soap.partner.FieldType.base64 && fieldSet.contains(f.getName.toLowerCase)).length > 0

            //Batch Apex does not support relationship fields and base64 fields
            val isAllowAsyncMode = !soqlParser.hasRelationshipFields && !hasBase64Fields && Set(AsyncWithGlobalWhere, AsyncWithoutGlobalWhere).contains(currentMode)
            var result = (currentMode, false)

            val canTryCurrentMode = isAllowAsyncMode || !allAsyncModes.contains(currentMode)

            if (canTryCurrentMode) {
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
                    val size = if (isAllowAsyncMode)
                        loadAsync(connection, objectApiName, soqlParser, queryString, fieldList, outputFilePath)
                    else
                        loadSync(connection, objectApiName, soqlParser, queryString, fieldList, outputFilePath)

                    onFileComplete(objectApiName, outputFilePath, size)
                    println(objectApiName + ": " + size + " " + {if (isAllowAsyncMode) "bytes" else "lines"})
                    result = (currentMode, true)
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
            }
            val modesToTry = if (!isAllowAsyncMode)
                availableModes.tail.filter(!allAsyncModes.contains(_))
            else
                availableModes.tail

            if (!result._2 && !modesToTry.isEmpty)
                runOneMode(modesToTry)
            else
                result
        }

        //finally - run the process
        if (Option("true") == Config.useBulkApi)
            runOneMode(List(AsyncWithGlobalWhere, AsyncWithoutGlobalWhere, SyncWithGlobalWhere, SyncWithoutGlobalWhere))
        else
            runOneMode(List(SyncWithGlobalWhere, SyncWithoutGlobalWhere))


    }




    private def processRecord(record: SObject) {
        //save attachment as file if "backup.attachment.asfile" is not null
        val FILE_OBJ_TYPES = Map("attachment" -> ("Name", "Body"), "document" -> ("Name", "Body"),
                                 "contentversion" -> ("PathOnClient", "VersionData"))
        val objectApiName = record.getType.toLowerCase
        if ( FILE_OBJ_TYPES.contains(objectApiName)) {
            val fileNameField = FILE_OBJ_TYPES(objectApiName)._1
            val fileBodyField = FILE_OBJ_TYPES(objectApiName)._2

            val fileName = Config.formatAttachmentFileName(record.getField(fileNameField), record.getId)

            if (null != fileName) {
                val file = new File(Config.mkdirs(record.getType) + File.separator + fileName)
                val buffer = record.getField(fileBodyField) match {
                    case null => Array[Byte]()
                    case x => x.toString.getBytes
                }
                if (buffer.length > 0) {
                    val output = new FileOutputStream(file)
                    output.write(Base64.decode(buffer))
                    output.close()
                }
            }
        }
    }

    def loadSync(connection: PartnerConnection, objectApiName: String, soqlParser: SOQLParser,  query: String,
             fieldList: List[String], outputFilePath: String):Long = {

        var writerNeedsClosing = false
        lazy val csvWriter = {
            //println("about to start:  " + outputFilePath)
            Config.HookEachBefore.execute(objectApiName, outputFilePath)
            val file = new File(outputFilePath)
            file.createNewFile()
            val writer = new FileWriter(file)
            writerNeedsClosing = true
            new com.sforce.bulk.CsvWriter(fieldList.toArray[String], writer)
        }
        var queryResults = if (soqlParser.isAllRows)
            connection.queryAll(query)
        else
            connection.query(query)
        val size = queryResults.getSize
        if (size > 0) {
            var doExit = false
            do {
                for (record <- queryResults.getRecords) {
                    //println("Id: " + record.getId + "; Name=" + record.getField("Name"))
                    val values = fieldList.map(fName => (record.getFieldIgnoreCase(fName) match {
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
        }
        //println(objectApiName + ": " + size)
        size
    }

    def onFileComplete(objectApiName: String, outputFilePath: String, size: Long) {
        if (size> 0) {
            Config.HookEachAfter.execute(objectApiName, outputFilePath, size)
        }
    }

    private def getBulkConnection(connection: PartnerConnection): BulkConnection = {
        val config = new com.sforce.ws.ConnectorConfig()
        config.setSessionId(connection.getConfig.getSessionId)

        val soapServiceEndpoint = connection.getConfig.getServiceEndpoint()
        val restEndpoint = soapServiceEndpoint.take(soapServiceEndpoint.indexOf("Soap/")) + "async/" + Config.apiVersion
        config.setRestEndpoint(restEndpoint)
        config.setCompression(true)

        new BulkConnection(config)
    }
    def loadAsync(connection: PartnerConnection, objectApiName: String, soqlParser: SOQLParser,  query: String,
                      fieldList: List[String], outputFilePath: String):Long = {
        var job = new JobInfo()
        job.setObject(objectApiName)

        job.setOperation(OperationEnum.query)
        job.setContentType(ContentType.CSV)
        job.setConcurrencyMode(ConcurrencyMode.Parallel)

        val bulkConnection = getBulkConnection(connection)
        job = bulkConnection.createJob(job)
        require(null != job.getId, "Failed to create Bulk Job")

        job = bulkConnection.getJobStatus(job.getId)

        val bout = new ByteArrayInputStream(query.getBytes)
        var info = bulkConnection.createBatchFromStream(job, bout)

        var queryResults: Array[String] = null
        var keepWaiting = true
        while (keepWaiting) {
            Thread.sleep(10 * 1000)
            info = bulkConnection.getBatchInfo(job.getId, info.getId)
            info.getState match {
                case BatchStateEnum.Completed =>
                    val list =  bulkConnection.getQueryResultList(job.getId, info.getId)
                    queryResults = list.getResult
                    keepWaiting = false
                case BatchStateEnum.Failed =>
                    keepWaiting = false
                    println(info.getStateMessage)
                    throw new BatchProcessingException(info.getStateMessage)
                case _ => //in progress
            }
        }
        var size = 0
        if (null != queryResults) {
            for (resultId <- queryResults) {
                val input = bulkConnection.getQueryResultStream(job.getId, info.getId, resultId)
                val file = new File(outputFilePath)
                file.createNewFile()
                val output = new FileOutputStream(file)
                val bytes = new Array[Byte](1024) //1024 bytes - Buffer size
                Iterator
                    .continually (input.read(bytes))
                    .takeWhile (-1 !=)
                    .foreach (read=>{output.write(bytes,0,read); size += read})
                output.close()

                if (size < 1 || size < 40 && scala.io.Source.fromFile(file).getLines().contains("Records not found for this query")) {
                    //no point in keeping empty files
                    file.delete()
                }

            }
            closeJob(bulkConnection, info.getJobId)
        }
        size

    }
    def closeJob(bulkConnection: BulkConnection, jobId: String) {
        val job = new JobInfo()
        job.setId(jobId)
        job.setState(JobStateEnum.Closed)
        bulkConnection.updateJob(job)

    }
}

