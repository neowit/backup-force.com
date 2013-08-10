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

import java.util.Properties
import java.io.{FileWriter, File}

class ConfigGenerator {
    //println(jarPath )
    println()
    println(
        """You started the utility without --config command line parameter.
    Would you like to:
    1. Generate config file interactively
    2. See list of command line options
    3. Exit
        """)
    readLine("Enter 1, 2 or 3: ") match {
        case "1" =>
            val configPath = requestFilePath()
            val config = requestProperties(configPath)
            //save config
            if (!config.isEmpty) {
                val writer = new FileWriter(configPath)
                config.store(writer, " backup-force.com configuration file \n see example of all possible properties at:\n https://github.com/neowit/backup-force.com/blob/master/config/sample-configuration.properties")
                println("\nYou can now start the process using following command line: \n")
                println("java -jar " + Config.jarPath + " --config " + new File(configPath).getAbsolutePath + "\n")
            }
        case "2" => Config.help(); System.exit(0)
        case _ => System.exit(0)
    }

    def requestFilePath() = {
        val pathExample = if (Config.isUnix) "/home/user/myconf.properties" else "c:/extract/myconf.properties"
        var fPath = ""
        while (null == fPath || fPath.isEmpty) {
            fPath = readLine( "Enter the full path and the name of the config file to be created. \nFor example: "+ pathExample + "\n" )
        }
        fPath
    }
    def requestProperties(configPath: String): PropertiesOption = {
        val template, config = new Properties() with PropertiesOption
        template.load(getClass.getResourceAsStream("/configuration-template.properties"))

        if (null != configPath) {
            //check if this is existing file
            val f:File = new File(configPath)
            if (f.exists()) {
                if (!f.canWrite) {
                    println("File " + configPath + " is not writable.")
                    System.exit(0)
                }

                println("It appears file " + configPath + " already exists.")
                if ("a" == readLine("Enter 'a' to abort the process, any other key - continue updating existing config: ")) {
                    System.exit(0)
                }
                config.load(scala.io.Source.fromFile(configPath).bufferedReader())

            }
        }

        val sections = template.getPropertyOption("sections").get.split(",")
        for (s <- sections; section = s.trim) {

            displaySectionHeader(section)
            displaySectionDescription(section)
            enterKeyValues(section)

        }
        println("RESULT")
        println(config)

        def displaySectionHeader(sectionKey: String) = {
            template.getPropertyOption(sectionKey + ".header") match {
                case Some(x) =>
                    println("\n========================================================")
                    println("# " + x)
                case None =>
            }
        }
        def displaySectionDescription(sectionKey: String) = {
            template.getPropertyOption(sectionKey + ".description") match {
                case Some(x) =>
                    println()
                    println(x)
                case None =>
            }
        }
        def enterKeyValues(sectionKey: String) = {
            val keys = template.getPropertyOption(sectionKey + ".list")  match {
              case Some(x) if !x.isEmpty=> x.split(",")
              case _ => Array[String]()
            }
            for (k <- keys; key = k.trim) {
                template.getPropertyOption(sectionKey + "." + key + ".header")  match {
                    case Some(x) =>
                        println()
                        println(x)
                    case None =>
                }
                template.getPropertyOption(sectionKey + "." + key + ".description")  match {
                    case Some(x) =>
                        println()
                        println(x)
                    case None =>
                }
                template.getPropertyOption(sectionKey + "." + key)  match {
                    case Some(x) if !x.isEmpty => println(key + " - Example value: " + x)
                    case None => ""
                    case _ => ""
                }
                //println()
                val currentValue = config.getPropertyOption(key) match {
                  case Some(x) if !x.isEmpty => " [Current value: " + x + "]"
                  case None => ""
                  case _ => ""
                }
                var noExit = true
                while (noExit) {
                    val res = readLine(key + currentValue + "=")
                    if (null != res && !res.isEmpty) {
                        config.setProperty(key, res)
                        noExit = false
                    } else if (!currentValue.isEmpty) {
                        //keep current value
                        noExit = false
                    } else if (Option("true") == template.getPropertyOption(sectionKey + ".required")) {
                        println(key + " is required")
                    } else {
                        noExit = false
                    }
                }
            }
        }
        config
    }
}

