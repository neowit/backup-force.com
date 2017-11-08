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

import com.sforce.soap.partner.DescribeGlobalSObjectResult
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration


object BackupRunner extends LazyLogging {

    type OptionMap = Map[String, Any]

    //some types can not be queried en masse, so just exclude those
    val EXCLUDED_TYPES = Set("") //Set("ContentDocumentLink", "Vote")

    def isAcceptable(sobj: DescribeGlobalSObjectResult):Boolean ={
        sobj.isCreateable && sobj.isQueryable && !EXCLUDED_TYPES.contains(sobj.getName)
    }
    private val appConfig = Config.getConfig

    def main(args: Array[String]) {
        logger.debug(s"command line arguments: ${args.mkString(" ")}")
        if (args.isEmpty) {
            new ConfigGenerator
        } else {
            try {
                appConfig.load(args.toList)
                run()
            } catch {
                case _: InvalidCommandLineException => appConfig.help()
                case ex: MissingRequiredConfigParameterException => logger.error(ex.getMessage)
            } finally {
                //appConfig.lastQueryPropsActor ! "exit"
            }
        }
    }

    def run() {
        new UsageReporter(appConfig).report()

        val config = new com.sforce.ws.ConnectorConfig()
        config.setUsername(appConfig.username)
        config.setPassword(appConfig.password)

        val endpoint = appConfig.soapEndpoint
        if (null != endpoint)
            config.setAuthEndpoint(endpoint)

        config.setCompression(true)

        val proxyHost = appConfig.getProperty("http.proxyHost")
        val proxyPort = appConfig.getProperty("http.proxyPort")
        if (proxyHost.isDefined && proxyPort.isDefined)
            config.setProxy(proxyHost.get, proxyPort.get.toInt)

        val proxyUsername = appConfig.getProperty("http.proxyUsername")
        if (proxyUsername.isDefined )
            config.setProxyUsername(proxyUsername.get)

        val proxyPassword = appConfig.getProperty("http.proxyPassword")
        if (proxyPassword.isDefined )
            config.setProxyPassword(proxyPassword.get)

        val ntlmDomain = appConfig.getProperty("http.ntlmDomain")
        if (ntlmDomain.isDefined )
            config.setNtlmDomain(ntlmDomain.get)

        val connectionTimeoutSecs = appConfig.getProperty("http.connectionTimeoutSecs")
        if (connectionTimeoutSecs.isDefined )
            config.setConnectionTimeout(connectionTimeoutSecs.get.toInt * 1000)

        val readTimeoutSecs = appConfig.getProperty("http.readTimeoutSecs")
        if (readTimeoutSecs.isDefined )
            config.setReadTimeout(readTimeoutSecs.get.toInt * 1000)

        val connection = com.sforce.soap.partner.Connector.newConnection(config)
        logger.info("Auth EndPoint: "+config.getAuthEndpoint)
        logger.info("Service EndPoint: "+config.getServiceEndpoint)
        logger.info("Username: "+config.getUsername)
        //logger.info("SessionId: "+config.getSessionId)

        //list of objects that have custom SOQL query defined
        val customSoqlObjects = appConfig.objectsWithCustomSoql

        val objListProperty = appConfig.backupObjects match {
            case Some("*") => Set("*")
            case Some(x) => x.replaceAll(" ", "").split(",").toSet[String]
            case None => Set.empty[String]
        }

        val mainSObjectsSet: Set[String] =
            if (Set("*") != objListProperty)
                objListProperty
            else
                connection.describeGlobal().getSobjects.filter(sobj => isAcceptable(sobj)).map(sobj => sobj.getName).toSet

        val allSobjectSet = (mainSObjectsSet ++ customSoqlObjects).filterNot(objName => appConfig.backupObjectsExclude.contains(objName.toLowerCase))

        require(allSobjectSet.nonEmpty, "config file contains no objects to backup")
        def runAllProcesses() {
            if (allSobjectSet.nonEmpty) {
                appConfig.HookGlobalBefore.execute()
            }
            if (!appConfig.useBulkApi) {
                for (objApiName <- allSobjectSet) {
                    val backuper = new BackupSObject(connection, objApiName)
                    backuper.run()
                }
            } else {
                def callSequentially(objApiNames: List[String], processes: List[Future[(OperationMode, Boolean)]]): List[Future[(OperationMode, Boolean)]] = {
                    objApiNames match {
                        case objApiName :: tail =>
                            val backuper = new BackupSObject(connection, objApiName)
                            val f = backuper.run()
                            callSequentially(tail, f :: processes)

                        case _ => processes
                    }

                }
                val futureList = callSequentially(allSobjectSet.toList, List[Future[(OperationMode, Boolean)]]() )

                import scala.concurrent.ExecutionContext.Implicits.global
                Await.result(Future.sequence(futureList), Duration.Inf)
            }
            if (allSobjectSet.nonEmpty) {
                appConfig.HookGlobalAfter.execute()
            }
        }
        Benchmark("useBulkApi = " + appConfig.useBulkApi) {
            runAllProcesses()
        }

    }

}

