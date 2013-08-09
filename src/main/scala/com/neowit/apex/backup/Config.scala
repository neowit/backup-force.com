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

trait PropertiesOption extends Properties{

    def getPropertyOption(key: String): Option[String] = {
        super.getProperty(key) match {
            case null => None
            case x => Some(x)
        }
    }
    def getPropertyOption(key: String, defaultValue: String): Option[String] = {
        super.getProperty(key) match {
            case null => Some(defaultValue)
            case x => Some(x)
        }
    }
}

object Config {
    def isUnix = {
        val os = System.getProperty("os.name").toLowerCase
        os.contains("nux") || os.contains("mac")
    }

    type OptionMap = Map[String, String]
    private val mainProps, credProps, lastQueryProps = new Properties() with PropertiesOption

    private var options:OptionMap = Map()
    private var isValidCommandLine = false
    private var lastQueryPropsFile: File = null

    def load(arglist: List[String]) {
        if (arglist.isEmpty) {
            throw new InvalidCommandLineException
        }

        @tailrec
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
            list match {
                case Nil => map
                case key :: value :: tail if key.startsWith("--") => key match {
                        case "--config" => nextOption(map ++ Map("config" -> value), tail)
                        case "--credentials" => nextOption(map ++ Map("credentials" -> value), tail)
                        case _ => nextOption(map ++ Map(key.drop(2) -> value), tail)
                }
                case value :: Nil =>
                    throw new InvalidCommandLineException
                case _ =>
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

        lastRunOutputFile match {
            case None => Unit
            case Some(fName) =>
                lastQueryPropsFile = new File(fName)
                if (!lastQueryPropsFile.exists) {
                    lastQueryPropsFile.createNewFile()
                }
                lastQueryProps.load(scala.io.Source.fromFile(fName).bufferedReader())

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
        [--<any param from config file>]: (optional) all config parameters can be specified in both config file and command line.
                        Command line parameters take precendence

        Example:
        java -jar backup-force.com-1.0-SNAPSHOT-jar-with-dependencies.jar --config ~/myconf.properties

        OR if sfdc login/pass are in a different file
        java -jar backup-force.com-1.0-SNAPSHOT-jar-with-dependencies.jar --config ~/myconf.properties \
            --credentials /Volumes/TRUECRYPT1/SForce.properties


        In the following example username user@domain.com specified in the command line will be used,
        regardless of whether it is also specified in config file or not
        java -jar backup-force.com-1.0-SNAPSHOT-jar-with-dependencies.jar --config ~/myconf.properties \
            --sf.username user@domain.com

                 """)
    }

    def getProperty(key:String):Option[String] = getProperty(key, None)

    def getProperty(key:String, defaultVal: Option[String]):Option[String] = {
        val cmdLineValue = options.get(key)
        val configValue = mainProps.getPropertyOption(key)
        val res = cmdLineValue match {
            case None => configValue match {
                    case None => defaultVal
                    case _ => configValue
                }
            case _ => cmdLineValue
        }
        evalShellCommands(res)
    }

    def getProperty(key:String, alterConf:PropertiesOption):Option[String] = {
        val cmdLineValue = options.get(key)
        val alterConfValue = alterConf.getPropertyOption(key)
        val mainConfValue = mainProps.getPropertyOption(key)

        val res = cmdLineValue match {
            case Some(cmdVal) => cmdLineValue
            case None =>  alterConfValue match {
                case Some(x) => alterConfValue
                case None => mainConfValue
            }
        }

        evalShellCommands(res)
    }

    /**
     * all parameters with prefix sf. and http. can be sourced from both
     * main config and credentials config
     */
    def getCredential(key: String): Option[String] = {
        try {
            getProperty(key, credProps)
        } catch {
            case ex: MissingConfigParameterException => None
        }
    }

    /**
     * if there are any shell commands then evaluate them and return result
     * @param str shell commands are denoted by `something ...` syntax
     */
    def evalShellCommands(str: Option[String]) = {
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

        str match {
            case None => None
            case Some(x) => Option(evalOne(x))
        }
    }
    lazy val username = getProperty("sf.username", credProps).getOrElse("")
    lazy val password = getProperty("sf.password", credProps).getOrElse("")
    lazy val endpoint = {
        val serverUrl = getProperty("sf.serverurl", credProps)
        serverUrl match {
            case Some(x) => x + "/services/Soap/u/28.0"
            case None => null
        }
    }
    lazy val backupObjects = getProperty("backup.objects")

    lazy val objectsWithCustomSoql = mainProps.stringPropertyNames().toArray.filter(propName => propName.toString.startsWith("backup.soql."))
        .map(propName => propName.toString.replace("backup.soql.", "")).toSet[String]


    //outputFolder value should be evaluated only once, otherwise different objects may end up in different folders
    lazy val outputFolder = getProperty("outputFolder").getOrElse(null)
    lazy val lastRunOutputFile = getProperty("lastRunOutputFile")

    lazy val globalWhere = getProperty("backup.global.where")

    lazy val hookGlobalBefore = getProperty("hook.global.before")
    lazy val hookGlobalAfter = getProperty("hook.global.after")
    lazy val hookEachBefore = getProperty("hook.each.before")
    lazy val hookEachAfter = getProperty("hook.each.after")

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

    abstract class Hook () {
        def scriptPath:Option[String]
        def defaultArgs:Seq[String] = {
            val outputFolderStr = outputFolder
            lastRunOutputFile match {
                case Some(lrof) => Seq(scriptPath.get, outputFolderStr, lrof)
                case None => Seq(scriptPath.get, outputFolderStr, "")
            }
        }

        def execute(args: Seq[String] = Seq()):Int = scriptPath match{
            case Some(path) => Process(path, defaultArgs ++ args).!
            case None => 0
        }
    }

    /**
     * hook.global.before - script run before process starts
     * params:
     * - full path to output folder
     * - full path to lastRunOutputFile
     */
    object HookGlobalBefore extends Hook {
        lazy val scriptPath = getProperty("hook.global.before")
    }

    object HookGlobalAfter extends Hook {
        lazy val scriptPath = getProperty("hook.global.after")
    }
    /**
     * hook.each.before - script run before each Object process starts
     * params:
     * - full path to output folder
     * - full path to lastRunOutputFile
     * - Object type name
     * - full path to <objecttype>.csv which will be created
     */
    object HookEachBefore extends Hook {
        lazy val scriptPath = getProperty("hook.each.before")
        def execute(objectApiName: String, outputCsvPath: String):Int = {
            execute(Seq(objectApiName, outputCsvPath))
        }
    }

    /**
     * hook.each.before - script run before each Object process starts
     * params:
     * - full path to output folder
     * - full path to lastRunOutputFile
     * - Object type name
     * - full path to <objecttype>.csv which will be created
     * - number of saved records
     */
    object HookEachAfter extends Hook {
        lazy val scriptPath = getProperty("hook.each.after")
        def execute(objectApiName: String, outputCsvPath: String, numOfRecords: Int):Int = {
            execute(Seq(objectApiName, outputCsvPath, numOfRecords.toString))
        }
    }
}

