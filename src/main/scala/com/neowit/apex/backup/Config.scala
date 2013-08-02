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
import java.io.{FileWriter, File}
import scala.sys.process._


class InvalidCommandLineException(msg: String)  extends IllegalArgumentException {
    def this() {
        this(null)
    }
}
class MissingConfigParameterException(msg:String) extends InvalidCommandLineException

object Config {

    type OptionMap = Map[String, Any]
    private val mainProps, credProps, lastQueryProps = new Properties()
    private var options:OptionMap = Map()
    private var isValidCommandLine = false
    private var lastQueryPropsFile: File = null

    def load(arglist: List[String]) {
        if (arglist.isEmpty) {
            help()
            throw new InvalidCommandLineException
        }

        @tailrec
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
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
                case "--outputFolder" :: value :: tail =>
                    nextOption(map ++ Map("outputFolder" -> value.toString), tail)
                case "--backup.objects" :: value :: tail =>
                    nextOption(map ++ Map("backup.objects" -> value.toString.split(',').map((s: String) => s.trim)), tail)
                case _ =>
                    help()
                    throw new InvalidCommandLineException
            }
        }

        //args.foreach(arg => println(arg))
        options = nextOption(Map(), arglist)
        isValidCommandLine = true
        println(options)
        val mainConfigPath = options("config")
        require(null != mainConfigPath, "missing --config parameter")

        mainProps.load(scala.io.Source.fromFile(mainConfigPath.toString).bufferedReader())

        val credentialsConfigPath = if (options.contains("credentials")) options("credentials") else mainConfigPath
        credProps.load(scala.io.Source.fromFile(credentialsConfigPath.toString).bufferedReader())

        //make sure output folder exists
        Config.mkdirs("")

        if (null != lastRunOutputFile) {
            lastQueryPropsFile = new File(lastRunOutputFile)
            if (!lastQueryPropsFile.exists) {
                lastQueryPropsFile.createNewFile()
            }
            lastQueryProps.load(scala.io.Source.fromFile(lastRunOutputFile).bufferedReader())
        }
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
      java -jar backup-force.com-1.0-SNAPSHOT-jar-with-dependencies.jar --config ~/myconf.properties
      OR
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
        val res = if (None != cmdLineValue) cmdLineValue.toString else configValue
        evalShellCommands(res)
    }
    def getProperty(propName:String, alterConf:Properties):String = {
        val cmdLineValue = options.get(propName)
        val alterConfValue = alterConf.getProperty(propName)
        val mainConfValue = mainProps.getProperty(propName)

        val res = if (None != cmdLineValue) cmdLineValue
                    else if (null != alterConfValue) alterConfValue
                    else mainConfValue

        if (null == res)
            throw new MissingConfigParameterException("missing parameter " + propName)

        evalShellCommands(res.toString)
    }

    /**
     * if there are any shell commands then evaluate them and return result
     * @param str shell commands are denoted by `something ...` syntax
     */
    def evalShellCommands(str: String) = {
        def evalOne(curStr: String): String = {
            val firstIndex = curStr.indexOf("`")
            val secondIndex = curStr.indexOf("`", firstIndex+1)
            if (firstIndex >=0 && secondIndex >0) {
                val expr = curStr.substring(firstIndex+1, secondIndex)
                //val res1 = expr.!!
                //remove end of line sequence if present
                //val result = "\n$".r.replaceFirstIn(res1, "")
                val result = Process(expr).lines.head
                evalOne(curStr.substring(0, firstIndex) + result + curStr.substring(secondIndex+1))

            } else curStr
        }

        if (null == str)
            str
        else
            evalOne(str)
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


    //outputFolder value should be evaluated only once, otherwise different objects may end up in different folders
    lazy val outputFolder = getProperty("outputFolder")
    lazy val lastRunOutputFile = getProperty("lastRunOutputFile")

    def globalWhere = getProperty("backup.global.where")


    def storeLastModifiedDate(objectApiName: String, formattedDateTime: String) {

        if (null != lastQueryPropsFile) {
            lastQueryProps.setProperty(objectApiName.toLowerCase, formattedDateTime)
            val writer = new FileWriter(lastQueryPropsFile)
            lastQueryProps.store(writer, "time stamps when last queried each object")
        }
    }

    def getStoredLastModifiedDate(objectApiName: String): String = {
        val storedDateStr = lastQueryProps.getProperty(objectApiName.toLowerCase)
        if (null != storedDateStr)
            storedDateStr
        else
            "1900-01-01T00:00:00Z"
    }

    /*
     * generates specified folders nested in the main outputFolder
     */
    def mkdirs(dirName: String) = {
        val path = outputFolder + File.separator + dirName
        //check that folder exists
        val f = new File(path)
        if (!f.isDirectory) {
            if (!f.mkdirs())
                throw new RuntimeException("Failed to create folder: " + path)
        }

        path
    }

}

