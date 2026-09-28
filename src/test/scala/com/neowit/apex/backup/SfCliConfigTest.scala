package com.neowit.apex.backup

import java.io.{File, FileWriter}
import java.util.Properties
import org.scalatest.funsuite.AnyFunSuite

class SfCliConfigTest extends AnyFunSuite {
    private def withFile(values: Map[String, String])(testCode: Config => Unit): Unit = {
        val file = File.createTempFile("sf-cli-config-test", ".properties")
        val writer = new FileWriter(file)
        try {
            val properties = new Properties()
            values.foreach { case (key, value) => properties.setProperty(key, value) }
            properties.store(writer, "")
            writer.close()
            val config = new Config
            config.load(List("--config", file.getAbsolutePath))
            testCode(config)
        } finally {
            writer.close()
            file.delete()
        }
    }

    test("orgAlias reads sf.orgAlias") {
        withFile(Map("sf.orgAlias" -> "backup-org")) { config =>
            assert(config.orgAlias == "backup-org")
        }
    }

    test("missing sf.orgAlias throws MissingRequiredConfigParameterException") {
        withFile(Map.empty) { config =>
            val error = intercept[MissingRequiredConfigParameterException](config.orgAlias)
            assert(error.getMessage.contains("sf.orgAlias"))
        }
    }

    test("empty sf.orgAlias throws MissingRequiredConfigParameterException") {
        withFile(Map("sf.orgAlias" -> "")) { config =>
            val error = intercept[MissingRequiredConfigParameterException](config.orgAlias)
            assert(error.getMessage.contains("sf.orgAlias"))
        }
    }

    test("cliPath defaults to sf") {
        withFile(Map("sf.orgAlias" -> "backup-org")) { config =>
            assert(config.cliPath == "sf")
        }
    }

    test("cliPath reads an override") {
        withFile(Map("sf.orgAlias" -> "backup-org", "sf.cliPath" -> "C:/tools/sf.exe")) { config =>
            assert(config.cliPath == "C:/tools/sf.exe")
        }
    }
}
