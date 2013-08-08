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

import java.util.{ResourceBundle, Properties}

class ConfigGenerator {

    println()
    println(
        """You started the utility without --config command line parameter.
    Would you like to:
    1. Generate config file interactively
    2. See list of command line options
    3. Exit and restart with --config command line parameter
        """)
    readLine("Enter 1, 2 or 3: ") match {
        case "1" =>
            val config = requestProperties()
            //save config
        case "2" => Config.help(); System.exit(0)
        case _ => System.exit(0)
    }


    def requestProperties(): PropertiesOption = {
        val template, config = new Properties() with PropertiesOption
        template.load(getClass.getResourceAsStream("/configuration-template.properties"))

        //template.load(scala.io.Source.fromFile("/resources/configuration-template.properties").bufferedReader())
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
                    println("========================================================")
                    println("# " + x)
                    println()
                case None =>
            }
        }
        def displaySectionDescription(sectionKey: String) = {
            template.getPropertyOption(sectionKey + ".description") match {
                case Some(x) =>
                    //println()
                    println(x)
                    println()
                case None =>
            }
        }
        def enterKeyValues(sectionKey: String) = {
            val keys = template.getPropertyOption(sectionKey + ".list").get.split(",")
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
                    case Some(x) =>
                        println()
                        println("Example value: " + x)
                    case None =>
                }
                //TODO use formatted readline(text: String, args: Any*)
                var noExit = true
                while (noExit) {
                    val res = readLine(key + "=")
                    if (null != res && !res.isEmpty) {
                        config.setProperty(key, res)
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

