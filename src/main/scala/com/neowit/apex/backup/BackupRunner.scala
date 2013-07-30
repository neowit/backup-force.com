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

object BackupRunner {

    type OptionMap = Map[String, Any]

    //some types can not be queried en masse, so just exclude those
    val EXCLUDED_TYPES = Set("ContentDocumentLink", "Vote")

    def isAcceptable(sobj: DescribeGlobalSObjectResult):Boolean ={
        sobj.isReplicateable && sobj.isCreateable && sobj.isQueryable && !EXCLUDED_TYPES.contains(sobj.getName)
    }

    def main(args: Array[String]) {
        if (args.isEmpty)
            Config.help()

        val arglist = args.toList
        Config.load(arglist)
        run()
    }

    def run() {

        val username = Config.username
        val password = Config.password
        val endpoint = Config.endpoint

        val config = new com.sforce.ws.ConnectorConfig()
        config.setUsername(username)
        config.setPassword(password)
        if (null != endpoint)
            config.setAuthEndpoint(endpoint)

        val connection = com.sforce.soap.partner.Connector.newConnection(config)
        println("Auth EndPoint: "+config.getAuthEndpoint)
        println("Service EndPoint: "+config.getServiceEndpoint)
        println("Username: "+config.getUsername)
        println("SessionId: "+config.getSessionId)

        //list of objects that have custom SOQL query defined
        val customSoqlObjects = Config.objectsWithCustomSoql

        val objListProperty = Config.backupObjects match {
            case "*" => Set("*")
            case x:String => x.replaceAll(" ", "").split(",").toSet[String]
            case null => Set()
        }
        val mainSObjectsSet =
            if (Set("*") != objListProperty)
                objListProperty
            else
                connection.describeGlobal().getSobjects.filter(sobj => isAcceptable(sobj)).map(sobj => sobj.getName).toSet

        val allSobjectSet = mainSObjectsSet ++ customSoqlObjects

        require(!allSobjectSet.isEmpty, "config file contains no objects to backup")
        for (objApiName <- allSobjectSet) {
            val backuper = new BackupSObject(connection, objApiName)
            backuper.run()
        }


    }
}

