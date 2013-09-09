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
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
import scala.concurrent._
import com.typesafe.scalalogging.slf4j.Logging

class InvalidCommandLineException(msg: String)  extends IllegalArgumentException(msg: String) {
    def this() {
        this(null)
    }
}
class MissingRequiredConfigParameterException(msg:String) extends IllegalArgumentException(msg: String)

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

object Config extends Logging {
    private var config: Config = new Config
    def getConfig = {
        config
    }
    def resetConfig = {
        //used in unit tests
        config = new Config
    }
}
class Config extends Logging {
    def isUnix = {
        val os = System.getProperty("os.name").toLowerCase
        os.contains("nux") || os.contains("mac")
    }
    def jarPath = {
        val path = BackupRunner.getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath
        path match {
            case x if x.endsWith(".jar") =>
                //on Win resource path is returned as: /C:/... , need to remove leading slash
                if (!isUnix && x.startsWith("/")) x.substring(1) else x
            case _ => "backup-force.com-0.1.jar"
        }
    }

    type OptionMap = Map[String, String]
    private val mainProps = new Properties() with PropertiesOption

    private var options:OptionMap = Map()
    private var isValidCommandLine = false

    def load(arglist: List[String]) {
        if (arglist.isEmpty) {
            throw new InvalidCommandLineException
        }

        @tailrec
        def nextOption(configFilePaths: ListBuffer[String], map: OptionMap, list: List[String]): (List[String], OptionMap) = {
            list match {
                case Nil => (configFilePaths.toList, map)
                case key :: value :: tail if key.startsWith("--") => key match {
                        case "--config" => nextOption(configFilePaths += value, map, tail)
                        case _ => nextOption(configFilePaths, map ++ Map(key.drop(2) -> value), tail)
                }
                case value :: Nil =>
                    throw new InvalidCommandLineException
                case _ =>
                    throw new InvalidCommandLineException
            }
        }

        val (configFilePaths:List[String], opts) = nextOption(ListBuffer[String](), Map(), arglist)
        options = opts
        isValidCommandLine = true
        //logger.debug(options)
        //merge config files
        require(!configFilePaths.isEmpty, "missing --config parameter")
        for (confPath <- configFilePaths) {
            val conf = new Properties()
            conf.load(scala.io.Source.fromFile(confPath.toString).bufferedReader())

            val keys = conf.keySet().iterator()
            while (keys.hasNext) {
                val key = keys.next.toString
                val value = conf.getProperty(key, "")
                if ("" != value) {
                    //overwrite existing value
                    mainProps.setProperty(key, value)
                }
            }

        }

        //lastRunOutputFile
    }



    def help() {
        println( """
Backup force.com utility.
 https://github.com/neowit/backup-force.com

Command line parameters"
 --help : show this text
 --config : path to config.properties
 [[--config : path to config.properties]: (optional) more than one "--config" is supported, non blank parameters of later --config take precendence
 [--<any param from config file>]: (optional) all config parameters can be specified in both config file and command line. Command line parameters take precendence

Example:
 java -jar """ + jarPath + """ --config c:/myconf.properties

OR if sfdc login/pass are in a different file
 java -jar """ + jarPath + """ --config c:/myconf.properties --config c:/credentials.properties


In the following example username user@domain.com specified in the command line will be used,
regardless of whether it is also specified in config file or not
 java -jar """ + jarPath + """ --config c:/myconf.properties --sf.username user@domain.com

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

    def getRequiredProperty(key: String): Option[String] = {
        getProperty(key) match {
          case Some(s) if !s.isEmpty => Some(s)
          case _ =>  throw new MissingRequiredConfigParameterException(key +" is required")
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
    lazy val username = getRequiredProperty("sf.username").get
    lazy val password = getRequiredProperty("sf.password").get
    val apiVersion = "28.0"
    lazy val soapEndpoint = {
        val serverUrl = getRequiredProperty("sf.serverurl")
        serverUrl match {
            case Some(x) => x + "/services/Soap/u/" + apiVersion
            case None => null
        }
    }
    lazy val backupObjects = getProperty("backup.objects")

    lazy val backupObjectsExclude:Set[String] = getProperty("backup.objects.exclude") match {
        case Some("*") => logger.warn("Wildcard in 'backup.objects.exclude' parameter is not supported"); Set()
        case Some(x) => x.replaceAll(" ", "").split(",").map(_.toLowerCase).toSet[String]
        case None => Set()
    }

    lazy val objectsWithCustomSoql = mainProps.stringPropertyNames().toArray.filter(propName => propName.toString.startsWith("backup.soql."))
        .map(propName => propName.toString.replace("backup.soql.", "")).toSet[String]


    //outputFolder value should be evaluated only once, otherwise different objects may end up in different folders
    lazy val outputFolder: String = getProperty("outputFolder") match {
      case Some(path) if !path.isEmpty =>
          //make sure output folder exists
          val f = new File(path)
          if (!f.isDirectory) {
              if (!f.mkdirs())
                  throw new InvalidCommandLineException("Failed to create outputFolder: " + path)
          }
          path
      case _ => null
    }
    private lazy val lastRunOutputFile = getProperty("lastRunOutputFile")

    private lazy val lastQueryPropsFile: Option[File] = {
        lastRunOutputFile match {
            case None => Option(null)
            case Some(fName) =>
                val file = new File(fName)
                //make sure folder exists
                val dir = file.getParentFile
                if (!dir.exists()) {
                    if (!dir.mkdirs())
                        throw new IllegalArgumentException("Failed to create folder: " + dir.getAbsolutePath + " for lastRunOutputFile " + fName)
                }
                if (!file.exists) {
                    file.createNewFile()
                }
                Option(file)
        }
    }

    private lazy val lastQueryProps = {
        lastQueryPropsFile match {
            case None => null //will never be used
            case Some(file) =>
                val props = new Properties() with PropertiesOption
                props.load(scala.io.Source.fromFile(file).bufferedReader())
                props
        }
    }

    lazy val globalWhere = getProperty("backup.global.where")

    //sometimes we need to stop Bulk API usage and fall back to normal API
    def stopBulkApi() {
        mainProps.setProperty("sf.useBulkApi", "false")
    }
    def useBulkApi = getProperty("sf.useBulkApi") match {
      case Some("true") => true
      case _ => false
    }
    //an alternative to the above is a somewhat shorter one-liner
    //import PartialFunction._
    //def useBulkApi = cond(getProperty("sf.useBulkApi")) { case Some("true") => true }

    private def attachmentNameTemplate = getProperty("backup.extract.file")
    lazy val hasAttachmentNameTemplate = attachmentNameTemplate match {
        case Some(str) if str.trim.length >0 => true
        case _ => false
    }

    /**
     * convert file name based on $name, $id, $ext placeholders specified in "backup.extract.file" parameter
     * if no template specified then user dow not want to save real files
     */
    def formatAttachmentFileName(fileNameFieldValue: Any, objId: String, extensionFieldValue: Any = null): String = {

        attachmentNameTemplate match {
            case Some(str) if str.trim.length >0 =>
                val fileName = fileNameFieldValue match {
                    case null => null
                    case x if !("" + x).trim.isEmpty => x.toString
                    case _ => null
                }
                if (null != fileName) {
                    val extIndex1 = fileName.lastIndexOf(".")
                    val extIndex = if (extIndex1 >= 0) extIndex1 else fileName.length
                    val name = fileName.substring(0, extIndex)
                    val ext = if (null != extensionFieldValue) "." + extensionFieldValue
                        else
                            if (extIndex < fileName.length) fileName.substring(extIndex) else ""
                    val res = try {
                        str.replaceAll("\\$name", Regex.quoteReplacement(name)).replaceAll("\\$id", Regex.quoteReplacement(objId)).replaceAll("\\$ext", Regex.quoteReplacement(ext))
                    } catch {
                        case ex: Throwable =>
                            logger.error("ERROR, failed to apply file name replacements")
                            logger.error("str=" + str)
                            logger.error("name=" + name + "; id=" + objId + "; ext=" + ext)
                            logger.error("" + ex)
                        null
                    }
                    res
                } else
                    null
            case _ => null
        }
    }

    def storeLastModifiedDate(objectApiName: String, formattedDateTime: String) {
        blocking {
            if (null != lastQueryProps) {
                lastQueryProps.setProperty(objectApiName.toLowerCase, formattedDateTime)
                val writer = new FileWriter(lastQueryPropsFile.get)
                lastQueryProps.store(writer, "time stamps when last queried each object")
            }
        }
    }

    def getStoredLastModifiedDate(objectApiName: String): String = {
        blocking {
            val storedDateStr = if (null != lastQueryProps) lastQueryProps.getProperty(objectApiName.toLowerCase) else null
            if (null != storedDateStr)
                storedDateStr
            else
                "1900-01-01T00:00:00Z"
        }
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
        def hookName: String
        def scriptPath = getProperty("hook." + hookName)
        def userProvidedScriptArgs:Seq[String] = getProperty("hook." + hookName + ".params") match {
          case Some(s) => s.split(" ").toSeq
          case None => Seq()
        }
        def defaultArgs:Seq[String] = {
            val outputFolderStr = outputFolder
            lastRunOutputFile match {
                case Some(lrof) => Seq(outputFolderStr, lrof)
                case None => Seq(outputFolderStr, "")
            }
        }

        def execute(args: Seq[String] = Seq()):Int = scriptPath match{
            case Some(path) => Process(path, userProvidedScriptArgs ++ defaultArgs ++ args).!
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
        lazy val hookName = "global.before"
    }

    object HookGlobalAfter extends Hook {
        lazy val hookName = "global.after"
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
        lazy val hookName = "each.before"
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
        lazy val hookName = "each.after"
        def execute(objectApiName: String, outputCsvPath: String, numOfRecords: Long):Int = {
            execute(Seq(objectApiName, outputCsvPath, numOfRecords.toString))
        }
    }
}

