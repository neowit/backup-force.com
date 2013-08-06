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

import org.scalatest.{FunSuite}
import java.io.{File, FileWriter, FileNotFoundException}
import scala.sys.process.Process
import java.util.Properties

class ConfigTest extends FunSuite {
    def isUnix = {
        val os = System.getProperty("os.name").toLowerCase
        os.contains("nux") || os.contains("mac")
    }
    val FAIL = false
    def withFile(testCode: (File, FileWriter) => Any) {
        val file = File.createTempFile("test", ".properties") // create the fixture
        val writer = new FileWriter(file)
        try {
            writer.write("ScalaTest is ") // set up the fixture
            testCode(file, writer) // "loan" the fixture to the test
        } finally {
            // clean up the fixture
            writer.close()
            file.delete()
        }
    }

    test("No Command Params") {
        try {
            Config.load(List())
        } catch {
            case ex: InvalidCommandLineException => println("OK")
            case ex: Throwable => assert(FAIL, "Expected InvalidCommandLineException for empty command line. " + ex)
        }
    }
    test("Key without value") {
        try {
            Config.load(List("--incorrect"))
        } catch {
            case ex: InvalidCommandLineException => println("OK")
            case ex: Throwable => assert(FAIL, "Expected InvalidCommandLineException for empty command line." + ex)
        }
    }
    test("No --config Param") {
        try {
            Config.load(List("--sf.username", "aaa@bb.cc"))
        } catch {
            case ex: NoSuchElementException => println("OK")
            case ex: Throwable => assert(FAIL, "Expected NoSuchElementException for missing config parameter. " + ex)
        }
    }
    test("Missing ConfigFile") {
        try {
            Config.load(List("--config", "/some/path"))
        } catch {
            case ex: FileNotFoundException => println("OK")
            case ex: Throwable => assert(FAIL, "Expected FileNotFoundException for missing config parameter. " + ex)
        }
    }

    test("Existing config, but missing parameter") {
        withFile { (file, writer) =>
            val props = new java.util.Properties()
            props.setProperty("sf.username", "aaa@bb.cc")
            props.store(writer, "")

            Config.load(List("--config", file.getAbsolutePath))
            expectResult(None) { Config.getProperty("password")  }
        }
    }
    test("Command Line - Key Without Value") {
        intercept[InvalidCommandLineException]{
            Config.load(List("--config"))
        }
    }
    test("Command Line - Key Without --") {
        intercept[InvalidCommandLineException]{
            Config.load(List("config", "val"))
        }
    }
    test("Parameter from command line take priority over config") {
        withFile { (file, writer) =>
            val props = new java.util.Properties()
            props.setProperty("param1", "val1")
            props.store(writer, "")

            Config.load(List("--config", file.getAbsolutePath, "--param1", "val2"))
            expectResult(Option("val2")) { Config.getProperty("param1")  }
        }
    }
    test("Parameter from --credentials take priority over config") {
        //Config.load(List("--config", "/some/path"))
        //Config.mainProps.setProperty("prop1", "value1")
        withFile { (file, writer) =>
            val props = new java.util.Properties()
            props.setProperty("sf.username", "val1")
            props.store(writer, "")

            Config.load(List("--config", file.getAbsolutePath))
            expectResult("val1") { Config.username }
        }
    }
    test("Parameter from command line take prioirty over --credentials and --config") {
        withFile { (file, writer) =>
            val props = new Properties() with PropertiesOption
            props.setProperty("sf.serverurl", "val1")
            props.store(writer, "")

            Config.load(List("--config", file.getAbsolutePath, "--credentials", file.getAbsolutePath, "--sf.serverurl", "val2"))
            expectResult(Some("val2")) { Config.getProperty("sf.serverurl", props) }
        }
    }
    test("ShellEvaluation") {
        if (isUnix) {
            val s1 = Option("nothing to evaluate")
            expectResult(s1) { Config.evalShellCommands(s1) }
            //assert(s1 == Config.evalShellCommands(s1))

            val s2 = Option("`broken string")
            expectResult(s2) { Config.evalShellCommands(s2) }

            val s3 = Option("`echo abcd`")
            assert(Option("abcd") == Config.evalShellCommands(s3))

            val s4 = Option("""`echo abcd`""")
            expectResult(Option("abcd")) { Config.evalShellCommands(s4) }

            val s5 = Option("""some text`echo abcd`""")
            expectResult(Option("some textabcd")) { Config.evalShellCommands(s5) }

            val s6 = Option("""text before`echo abcd`text after""")
            expectResult(Option("text beforeabcdtext after")) { Config.evalShellCommands(s6) }

            val s7 = Option("""text before`echo abcd`text after`echo efgh`""")
            expectResult(Option("text beforeabcdtext afterefgh")) { Config.evalShellCommands(s7) }

            val now = Process("date +%Y%m%d-%H%M").lines.head
            val s9 = Option("I:/SFDC-Exports/backup-force.com/extracts/`date +%Y%m%d-%H%M`")
            expectResult(Option("I:/SFDC-Exports/backup-force.com/extracts/" + now)) { Config.evalShellCommands(s9) }
        }

    }

}
