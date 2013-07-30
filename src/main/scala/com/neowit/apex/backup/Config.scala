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

import scala.annotation.tailrec
import java.util.Properties


object Config {

    type OptionMap = Map[String, Any]
    private val mainProps, credProps = new Properties()
    private var options:OptionMap = Map()

    def load(arglist: List[String]) {
        if (arglist.isEmpty)
            help()

        @tailrec
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
            def isSwitch(s: String) = s(0) == '-'
            list match {
                case Nil => map
                case "--config" :: value :: tail =>
                    nextOption(map ++ Map("config" -> value.toString), tail)
                case "--credentials" :: value :: tail =>
                    nextOption(map ++ Map("credentials" -> value.toString), tail)
                case "--sf.username" :: value :: tail =>
                    nextOption(map ++ Map("sf.username" -> value.toString), tail)
                case "--sf.password" :: value :: tail =>
                    nextOption(map ++ Map("sf.password" -> value.toString), tail)
                case "--sf.endpoint" :: value :: tail =>
                    nextOption(map ++ Map("sf.endpoint" -> value.toString), tail)
                case "--backup.objects" :: value :: tail =>
                    nextOption(map ++ Map("backup.objects" -> value.toString.split(',').map((s: String) => s.trim)), tail)
                case string :: opt2 :: tail if isSwitch(opt2) =>
                    nextOption(map ++ Map("infile" -> string), list.tail)
                case string :: Nil => nextOption(map ++ Map("infile" -> string), list.tail)
                case option :: tail => println("Unknown or incomplete option: " + option)
                    help()
                    sys.exit(1)
            }
        }

        //args.foreach(arg => println(arg))
        options = nextOption(Map(), arglist)
        println(options)
        val mainConfigPath = options("config")
        require(null != mainConfigPath, "missing --config parameter")

        mainProps.load(scala.io.Source.fromFile(mainConfigPath.toString).bufferedReader())

        val credentialsConfigPath = if (options.contains("credentials")) options("credentials") else mainConfigPath
        credProps.load(scala.io.Source.fromFile(credentialsConfigPath.toString).bufferedReader())
    }



    def help() {
        println( """
      Backup force.com utility.
      https://github.com/neowit/backup-force.com

      Command line parameters"
      --help : show this text
      --config : path to config.properties
      --credentials : (optional)alternate config which contains login credentials

      If both --conf and any of the following command line parameters are specified then command line param value
      overrides config.properties value

      --sf.username: SFDC user name
      --sf.password: SFDC user password (and token if required)
      --sf.endpoint: sfdc endpoint, e.g. https://login.salesforce.com
      --backup.objects: comma separated list of SFDC object API names enclosed in double quotes
                     e.g.: --backup.objects "Account, Contact, MyObject__c"

      Example:
      java -jar backup-force.com --conf ~/myconf.properties
      java -classpath backup-force.com-1.0-SNAPSHOT-jar-with-dependencies.jar com.neowit.apex.backup.BackupRunner \
            --config /full/path/to/config/main.conf

      OR if sfdc login/pass are in a different file, e.g. Dataloader's config.properties

      java -classpath backup-force.com-1.0-SNAPSHOT-jar-with-dependencies.jar com.neowit.apex.backup.BackupRunner \
            --config /full/path/to/config/main.conf \
            --credentials /Volumes/TRUECRYPT1/SForce.properties

                 """)
    }

    def getProperty(propName:String):String = {
        val cmdLineValue = options.get(propName)
        val configValue = mainProps.getProperty(propName)
        if (None != cmdLineValue) cmdLineValue.toString else configValue
    }
    def getProperty(propName:String, alterConf:Properties):String = {
        val cmdLineValue = options.get(propName)
        val alterConfValue = alterConf.getProperty(propName)
        val mainConfValue = mainProps.getProperty(propName)
        if (None != cmdLineValue) cmdLineValue.toString
        else if (null != alterConfValue) alterConfValue
        else mainConfValue
    }
    def username = getProperty("sf.username", credProps)
    def password = getProperty("sf.password", credProps)
    def endpoint = {
        val serverUrl = getProperty("sf.serverurl", credProps)
        if (null != serverUrl)
            serverUrl + "/services/Soap/u/28.0"
        else
            null
    }
    def backupObjects = getProperty("backup.objects")

    def objectsWithCustomSoql = mainProps.stringPropertyNames().toArray.filter(propName => propName.toString.startsWith("backup.soql."))
        .map(propName => propName.toString.replace("backup.soql.", "")).toSet[String]

    def outputFolder = getProperty("outputFolder")

    def globalWhere = getProperty("backup.global.where")
}

