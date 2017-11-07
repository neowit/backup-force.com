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

import org.scalatest.{FunSuite, PrivateMethodTester}
import java.io.{File, FileWriter, FileNotFoundException}
import scala.sys.process.Process
import java.util.Properties

class ConfigTest extends FunSuite with PrivateMethodTester {
    private val appConfig = Config.getConfig
    private val isUnix = appConfig.isUnix

    val FAIL = false
    def withFile(testCode: (File, FileWriter) => Any) {
        val file = File.createTempFile("test", ".properties") // create the fixture
        val writer = new FileWriter(file)
        try {
            testCode(file, writer) // "loan" the fixture to the test
        } finally {
            // clean up the fixture
            writer.close()
            file.delete()
        }
    }

    test("No Command Params") {
        try {
            appConfig.load(List())
        } catch {
            case ex: InvalidCommandLineException => println("OK")
            case ex: Throwable => assert(FAIL, "Expected InvalidCommandLineException for empty command line. " + ex)
        }
    }
    test("Key without value") {
        try {
            appConfig.load(List("--incorrect"))
        } catch {
            case ex: InvalidCommandLineException => println("OK")
            case ex: Throwable => assert(FAIL, "Expected InvalidCommandLineException for empty command line." + ex)
        }
    }
    test("No --config Param") {
        intercept[IllegalArgumentException] {
            appConfig.load(List("--sf.username", "aaa@bb.cc"))
        }
    }
    test("Missing ConfigFile") {
        try {
            appConfig.load(List("--config", "/some/path"))
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

            appConfig.load(List("--config", file.getAbsolutePath))
            assertResult(None) { appConfig.getProperty("password")  }
        }
    }
    test("Command Line - Key Without Value") {
        intercept[InvalidCommandLineException]{
            appConfig.load(List("--config"))
        }
    }
    test("Command Line - Key Without --") {
        intercept[InvalidCommandLineException]{
            appConfig.load(List("config", "val"))
        }
    }
    test("Parameter from command line take priority over config") {
        withFile { (file, writer) =>
            val props = new java.util.Properties()
            props.setProperty("param1", "val1")
            props.store(writer, "")

            appConfig.load(List("--config", file.getAbsolutePath, "--param1", "val2"))
            assertResult(Option("val2")) { appConfig.getProperty("param1")  }
        }
    }
    test("Parameter from second --config takes priority over first --config") {
        withFile { (file, writer) =>
            val props = new java.util.Properties()
            props.setProperty("sf.username", "val1")
            props.store(writer, "")
            withFile {(f2, w2) =>
                val props2 = new Properties() with PropertiesOption
                props2.setProperty("sf.serverurl", "val2")
                props2.store(writer, "")

                appConfig.load(List("--config", file.getAbsolutePath, "--config", f2.getAbsolutePath))
                assertResult(Some("val2")) { appConfig.getProperty("sf.serverurl") }
            }

        }
    }
    test("Parameter from command line take priority over --config") {
        withFile { (file, writer) =>
            val props = new Properties() with PropertiesOption
            props.setProperty("sf.serverurl", "val1")
            props.store(writer, "")

            appConfig.load(List("--config", file.getAbsolutePath, "--config", file.getAbsolutePath, "--sf.serverurl", "val2"))
            assertResult(Some("val2")) { appConfig.getProperty("sf.serverurl") }
        }
    }
    test("ShellEvaluation") {
        if (isUnix) {
            val s1 = Option("nothing to evaluate")
            assertResult(s1) { appConfig.evalShellCommands(s1) }
            //assert(s1 == appConfig.evalShellCommands(s1))

            val s2 = Option("`broken string")
            assertResult(s2) { appConfig.evalShellCommands(s2) }

            val s3 = Option("`echo abcd`")
            assert(Option("abcd") == appConfig.evalShellCommands(s3))

            val s4 = Option("""`echo abcd`""")
            assertResult(Option("abcd")) { appConfig.evalShellCommands(s4) }

            val s5 = Option("""some text`echo abcd`""")
            assertResult(Option("some textabcd")) { appConfig.evalShellCommands(s5) }

            val s6 = Option("""text before`echo abcd`text after""")
            assertResult(Option("text beforeabcdtext after")) { appConfig.evalShellCommands(s6) }

            val s7 = Option("""text before`echo abcd`text after`echo efgh`""")
            assertResult(Option("text beforeabcdtext afterefgh")) { appConfig.evalShellCommands(s7) }

            val now = Process("date +%Y%m%d-%H%M").lines.head
            val s9 = Option("I:/SFDC-Exports/backup-force.com/extracts/`date +%Y%m%d-%H%M`")
            assertResult(Option("I:/SFDC-Exports/backup-force.com/extracts/" + now)) { appConfig.evalShellCommands(s9) }
        }

    }

    test("File name expansion - no backup.extract.file parameter") {
        withFile { (file, writer) =>
        //val props = new Properties() with PropertiesOption
        //props.store(writer, "")

        //template not provided
            appConfig.load(List("--config", file.getAbsolutePath, "--backup.extract.file", "" ))
            assertResult(null) { appConfig.formatAttachmentFileName("picture.jpg", "123456") }

        }
    }
    test("File name expansion - with backup.extract.file parameter 1") {
        withFile { (file, writer) =>
            //val props = new Properties() with PropertiesOption
            //props.store(writer, "")

            appConfig.load(List("--config", file.getAbsolutePath, "--backup.extract.file", """$id-$name$ext""" ))
            //no file name
            assertResult(null) { appConfig.formatAttachmentFileName(null, "123456") }
            //blank file name
            assertResult(null) { appConfig.formatAttachmentFileName("", "123457") }
            //empty file name
            assertResult(null) { appConfig.formatAttachmentFileName(" ", "123458") }
            //file name without extension
            assertResult("123456-picture") { appConfig.formatAttachmentFileName("picture", "123456") }
            //file name with extension
            assertResult("123456-picture.jpg") { appConfig.formatAttachmentFileName("picture.jpg", "123456") }
            //file name with two dots
            assertResult("123456-picture.abc.jpg") { appConfig.formatAttachmentFileName("picture.abc.jpg", "123456") }
            //file name with empty extension
            assertResult("123456-picture.") { appConfig.formatAttachmentFileName("picture.", "123456") }
            //file name with provided extension
            assertResult("123456-picture.doc") { appConfig.formatAttachmentFileName("picture.", "123456", "doc") }
            assertResult("123456-picture.doc") { appConfig.formatAttachmentFileName("picture", "123456", "doc") }
            assertResult("123456-Delivery_Action_Plan_v0_6.doc") { appConfig.formatAttachmentFileName("Delivery_Action_Plan_v0_6", "123456", "doc") }
        }
    }
    test("File name expansion - with backup.extract.file parameter 2") {
        withFile { (file, writer) =>
        //val props = new Properties() with PropertiesOption
        //props.store(writer, "")

            appConfig.load(List("--config", file.getAbsolutePath, "--backup.extract.file", """$id$ext""" ))
            //no file name
            assertResult(null) { appConfig.formatAttachmentFileName(null, "123456") }
            //blank file name
            assertResult(null) { appConfig.formatAttachmentFileName("", "123457") }
            //empty file name
            assertResult(null) { appConfig.formatAttachmentFileName(" ", "123458") }
            //file name without extension
            assertResult("123456") { appConfig.formatAttachmentFileName("picture", "123456") }
            //file name with extension
            assertResult("123456.jpg") { appConfig.formatAttachmentFileName("picture.jpg", "123456") }
            //file name with two dots
            assertResult("123456.jpg") { appConfig.formatAttachmentFileName("picture.abc.jpg", "123456") }
            //file name with empty extension
            assertResult("123456.") { appConfig.formatAttachmentFileName("picture.", "123456") }
        }
    }
    test("lastRunOutputFile - when it is not defined storeLastModifiedDate() should not fail") {
        withFile { (file, writer) =>
            appConfig.load(List("--config", file.getAbsolutePath))
            val storeLastModifiedDate = PrivateMethod[(String, String)]('storeLastModifiedDate)
            //next line must not fail with NPE or similar exception
            appConfig invokePrivate storeLastModifiedDate("dummyObject__c", "some-date")

        }
    }
    test("lastRunOutputFile - when it is not defined getStoredLastModifiedDate(objApiName) should return default value") {
        withFile { (file, writer) =>
            appConfig.load(List("--config", file.getAbsolutePath))
            val getStoredLastModifiedDate = PrivateMethod[String]('getStoredLastModifiedDate)
            val res = appConfig invokePrivate getStoredLastModifiedDate("dummyObject__c")
            assertResult("1900-01-01T00:00:00Z") {res}

        }
    }
    test("backup.objects & backup.objects.exclude") {
        withFile { (file, writer) =>
            //template not provided
            appConfig.load(List("--config", file.getAbsolutePath, "--backup.objects", "Account, Contact, Opportunity, Campaign"))
            assertResult(Set()) { appConfig.backupObjectsExclude }
            Config.resetConfig
            val appConfig2 = Config.getConfig
            appConfig2.load(List("--config", file.getAbsolutePath, "--backup.objects", "Account, Contact, Opportunity, Campaign", "--backup.objects.exclude" , "Opportunity, Contact" ))
            assertResult(Set("opportunity", "contact" )) { appConfig2.backupObjectsExclude }

        }
    }
}
