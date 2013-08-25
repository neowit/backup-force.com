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
import com.sforce.soap.partner.sobject.SObject
import java.io._
import com.sforce.ws.util.Base64
import com.sforce.async._
import com.sforce.ws.bind.XmlObject
import scala.annotation.tailrec
import scala.collection.JavaConversions._

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
                val itemName = child.getName.getLocalPart
                if ("type" != itemName && localName.equalsIgnoreCase(itemName)) {
                    Option(child)
                } else {
                    findFirst(it)
                }
            } else
                None
        }
        findFirst(record.getChildren)
    }

    def hasField(name: String): Boolean = {
        getFieldIgnoreCase(name) match {
            case null => false
            case x => true
        }
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

sealed trait OperationMode {
    //provide conversion of SObject to FieldResolver
    implicit def toFieldResolver(record: SObject) = new FieldResolver(record)
    val appConfig = Config.getConfig

    val BATCH_CHECK_INTERVAL_SEC = 15 //check if batch is ready to download every N seconds
    def isApplicable(objectApiName: String): Boolean
    def isReallyApplicable(soqlParser: SOQLParser, fields: Array[com.sforce.soap.partner.Field]): Boolean = {
        isApplicable(soqlParser.from)
    }
    val allowGlobalWhere: Boolean
    val isAsync: Boolean = false

    def load(connection: PartnerConnection, objectApiName: String, soqlParser: SOQLParser,  query: String,
                      fieldList: List[String], outputFilePath: String):Long

    protected def processRecord(record: SObject) {
        //save attachment as file if "backup.attachment.asfile" is not null
        val FILE_OBJ_TYPES = Map("attachment" -> ("Name", "Body", ""), "document" -> ("DeveloperName", "Body", "Type"),
                                 "contentversion" -> ("PathOnClient", "VersionData", ""),
                                "*feed" -> ("ContentFileName", "ContentData", "")
        )
        val (fileNameField, fileBodyField, fileExtensionField) = FILE_OBJ_TYPES.get(record.getType.toLowerCase) match {
            case Some((fNameFld, fBodyFld, fExtFld)) => (fNameFld, fBodyFld, fExtFld)
            case _ => //check if this is a feed item
                val (fNameFld, fBodyFld, fExtFld) = FILE_OBJ_TYPES("*feed")
                if (record.hasField(fNameFld) && record.hasField(fBodyFld))
                    (fNameFld, fBodyFld, fExtFld)
                else
                    (null, null, null)
        }
        if ( null != fileNameField && null != fileBodyField) {

            val fileName = appConfig.formatAttachmentFileName(record.getField(fileNameField), record.getId, record.getField(fileExtensionField))

            if (null != fileName) {
                //remove all characters '\/:*?' in file name because they may make file path invalid
                val cleanedFileName ="[\\\\|:|/\\*\\?]".r.replaceAllIn(fileName, "")
                val file = new File(appConfig.mkdirs(record.getType) + File.separator + cleanedFileName)
                val buffer = record.getField(fileBodyField) match {
                    case null => Array[Byte]()
                    case x => x.toString.getBytes
                }
                if (buffer.length > 0) {
                    try {
                        val output = new FileOutputStream(file)
                        output.write(Base64.decode(buffer))
                        output.close()
                    } catch {
                        case ex: FileNotFoundException =>
                            println("Error: Unable to save file: '" + fileName + "'\n using path: " + file.getAbsolutePath)
                            println(ex.getMessage)
                    }
                }
            }
        }
    }
}

class AsyncMode extends OperationMode {
    override def isApplicable(objectApiName: String): Boolean = {
        appConfig.useBulkApi
    }
    override def isReallyApplicable(soqlParser: SOQLParser, fields: Array[com.sforce.soap.partner.Field]): Boolean = {
        //Batch Apex does not support relationship fields and base64 fields
        val hasBase64Fields = fields.filter(f => f.getType == com.sforce.soap.partner.FieldType.base64).length > 0
        !soqlParser.hasRelationshipFields && !hasBase64Fields && isApplicable(soqlParser.from)
    }
    val allowGlobalWhere = true
    override val isAsync: Boolean = true

    def load(connection: PartnerConnection, objectApiName: String, soqlParser: SOQLParser,  query: String,
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
            Thread.sleep(BATCH_CHECK_INTERVAL_SEC * 1000)
            info = bulkConnection.getBatchInfo(job.getId, info.getId)
            info.getState match {
                case BatchStateEnum.Completed =>
                    val list =  bulkConnection.getQueryResultList(job.getId, info.getId)
                    queryResults = list.getResult
                    keepWaiting = false
                case BatchStateEnum.Failed =>
                    keepWaiting = false
                    //println(info.getStateMessage)
                    closeJob(bulkConnection, info.getJobId)
                    throw new BatchProcessingException("Warning: Bulk API call ["+ this.toString +"] for " + objectApiName +
                                                        " was unsuccessful. Will try another method...\n::Original error description:\n\t" +
                                                        info.getStateMessage + "\n")
                case _ => //in progress
            }
        }
        var size = 0
        if (null != queryResults) {
            for (resultId <- queryResults) {
                val input = bulkConnection.getQueryResultStream(job.getId, info.getId, resultId)
                var writerNeedsClosing = false
                lazy val output = {
                    //println("about to start:  " + outputFilePath)
                    //run user's before hook
                    appConfig.HookEachBefore.execute(objectApiName, outputFilePath)
                    writerNeedsClosing = true
                    val file = new File(outputFilePath)
                    file.createNewFile()
                    new FileOutputStream(file)
                }
                //check if file is empty
                val emptyMsg = "Records not found for this query"
                val emptySize = emptyMsg.getBytes.length
                val startBytes = new Array[Byte](emptySize)
                val readBytesSize = input.read(startBytes)
                if (readBytesSize > emptySize || new String(startBytes).indexOf(emptyMsg.substring(0, readBytesSize)) < 0) {
                    //file is not empty
                    output.write(startBytes,0,readBytesSize)
                    size += readBytesSize
                    //read the rest of the file
                    val bytes = new Array[Byte](1024) //1024 bytes - Buffer size
                    Iterator
                        .continually (input.read(bytes))
                        .takeWhile (-1 !=)
                        .foreach (read=>{output.write(bytes,0,read); size += read})

                    if (writerNeedsClosing) {
                        output.close()
                    }

                }
            }
            //closeJob(bulkConnection, info.getJobId)
        }
        try {
            closeJob(bulkConnection, info.getJobId)
        } catch {
            case ex: Throwable =>
                println("failed to close job " + objectApiName)
                println(ex)
                println(ex.getStackTraceString)
        }
        size

    }

    private def getBulkConnection(connection: PartnerConnection): BulkConnection = {
        val config = new com.sforce.ws.ConnectorConfig()
        config.setSessionId(connection.getConfig.getSessionId)

        val soapServiceEndpoint = connection.getConfig.getServiceEndpoint
        val restEndpoint = soapServiceEndpoint.take(soapServiceEndpoint.indexOf("Soap/")) + "async/" + appConfig.apiVersion
        config.setRestEndpoint(restEndpoint)
        config.setCompression(true)

        new BulkConnection(config)
    }
    private def closeJob(bulkConnection: BulkConnection, jobId: String) {
        val job = new JobInfo()
        job.setId(jobId)
        job.setState(JobStateEnum.Closed)
        bulkConnection.updateJob(job)

    }
}

abstract class SyncMode extends OperationMode {
    override def isApplicable(objectApiName: String): Boolean = {
        true
    }
    def load(connection: PartnerConnection, objectApiName: String, soqlParser: SOQLParser,  query: String,
             fieldList: List[String], outputFilePath: String):Long = {

        var writerNeedsClosing = false
        lazy val csvWriter = {
            //println("about to start:  " + outputFilePath)
            appConfig.HookEachBefore.execute(objectApiName, outputFilePath)
            val file = new File(outputFilePath)
            file.createNewFile()
            val writer = new PrintWriter(file, "UTF-8")
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
}
case object AsyncWithGlobalWhere extends AsyncMode {
    override def isApplicable(objectApiName: String): Boolean = {
        val configSoql = appConfig.getProperty("backup.soql." + objectApiName)
        super.isApplicable(objectApiName) &&  None == configSoql && None != appConfig.globalWhere
    }
    override val allowGlobalWhere = true
}

case object AsyncWithoutGlobalWhere extends AsyncMode {
    override val allowGlobalWhere = false

}
case object SyncWithGlobalWhere extends SyncMode {
    override def isApplicable(objectApiName: String): Boolean = {
        val configSoql = appConfig.getProperty("backup.soql." + objectApiName)
        None == configSoql && None != appConfig.globalWhere
    }
    override val allowGlobalWhere = true
}
case object SyncWithoutGlobalWhere extends SyncMode {
    override val allowGlobalWhere = false
}

