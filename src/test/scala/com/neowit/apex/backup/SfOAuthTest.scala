package com.neowit.apex.backup

import org.scalatest.funsuite.AnyFunSuite
import scala.sys.process._
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class SfCliAuthTest extends AnyFunSuite {
    test("buildCommand uses the configured executable and org alias") {
        assert(SfCliAuth.buildCommand("sf", "backup-org", windows = false) ==
            Seq("sf", "org", "display", "--target-org", "backup-org", "--json"))
        assert(SfCliAuth.buildCommand("C:/tools/sf.exe", "admin@example.invalid", windows = false) ==
            Seq("C:/tools/sf.exe", "org", "display", "--target-org", "admin@example.invalid", "--json"))
    }

    test("buildCommand quotes a single Windows command string for spaced aliases") {
        assert(SfCliAuth.buildCommand("sf", "SForce (tooling-force.com) SFDX", windows = true) ==
            Seq("cmd.exe", "/S", "/C", "sf org display --target-org \"SForce (tooling-force.com) SFDX\" --json"))
    }

    test("buildTokenCommand builds the access-token command for both platforms") {
        assert(SfCliAuth.buildTokenCommand("sf", "backup-org", windows = false) ==
            Seq("sf", "org", "auth", "show-access-token", "--target-org", "backup-org", "--json", "--no-prompt"))
        assert(SfCliAuth.buildTokenCommand("sf", "SForce (tooling-force.com) SFDX", windows = true) ==
            Seq("cmd.exe", "/S", "/C", "sf org auth show-access-token --target-org \"SForce (tooling-force.com) SFDX\" --json --no-prompt"))
    }

    test("buildCommand quotes spaced Windows executable paths and aliases in one string") {
        assert(SfCliAuth.buildCommand("c:/Program Files/sf/bin/sf.cmd", "SForce (tooling-force.com) SFDX", windows = true) ==
            Seq("cmd.exe", "/S", "/C", "\"c:/Program Files/sf/bin/sf.cmd\" org display --target-org \"SForce (tooling-force.com) SFDX\" --json"))
    }

    test("buildCommand leaves bare Windows executable names and simple aliases unquoted") {
        assert(SfCliAuth.buildCommand("sf", "backup-org", windows = true) ==
            Seq("cmd.exe", "/S", "/C", "sf org display --target-org backup-org --json"))
    }

    test("buildCommand quotes Windows metacharacters") {
        assert(SfCliAuth.buildCommand("foo(bar)", "backup-org", windows = true)(3) ==
            "\"foo(bar)\" org display --target-org backup-org --json")
        assert(SfCliAuth.buildCommand("sf", "foo(bar)", windows = true)(3) ==
            "sf org display --target-org \"foo(bar)\" --json")
    }

    test("buildCommand leaves spaced executable paths raw on non-Windows platforms") {
        val command = SfCliAuth.buildCommand("c:/Program Files/sf/bin/sf", "backup-org", windows = false)
        assert(command.head == "c:/Program Files/sf/bin/sf")
    }

    test("buildCommand leaves simple Windows org aliases unquoted") {
        assert(SfCliAuth.buildCommand("sf", "backup-org", windows = true) ==
            Seq("cmd.exe", "/S", "/C", "sf org display --target-org backup-org --json"))
    }

    test("buildCommand keeps the two-argument call using the platform default") {
        val command = SfCliAuth.buildCommand("sf", "backup-org")
        if (System.getProperty("os.name").toLowerCase.contains("win"))
            assert(command == SfCliAuth.buildCommand("sf", "backup-org", windows = true))
        else
            assert(command == SfCliAuth.buildCommand("sf", "backup-org", windows = false))
    }

    test("buildCommand adds cmd.exe prefix only on Windows") {
        assert(SfCliAuth.buildCommand("sf", "backup-org", windows = true).take(3) == Seq("cmd.exe", "/S", "/C"))
        assert(SfCliAuth.buildCommand("sf", "backup-org", windows = false).head == "sf")
    }

    test("Windows command probe preserves spaced paths and aliases through cmd") {
        assume(System.getProperty("os.name").toLowerCase.contains("win"))
        val directory = Files.createTempDirectory("sf probe ")
        val probeCmd = directory.resolve("probe.cmd")
        val script = "@echo off\r\necho A1=[%~1]\r\necho A2=[%~2]\r\necho A3=[%~3]\r\necho A4=[%~4]\r\necho A5=[%~5]\r\necho A6=[%~6]\r\necho A7=[%~7]\r\n"
        Files.write(probeCmd, script.getBytes(StandardCharsets.US_ASCII))
        val output = Process(SfCliAuth.buildCommand(probeCmd.toString, "SForce (tooling-force.com) SFDX", windows = true)).!!
        val lines = output.linesIterator.toList
        assert(lines.contains("A1=[org]"))
        assert(lines.contains("A4=[SForce (tooling-force.com) SFDX]"))
        assert(lines.contains("A5=[--json]"))
        val tokenOutput = Process(SfCliAuth.buildTokenCommand(probeCmd.toString, "SForce (tooling-force.com) SFDX", windows = true)).!!
        val tokenLines = tokenOutput.linesIterator.toList
        assert(tokenLines.contains("A1=[org]"))
        assert(tokenLines.contains("A2=[auth]"))
        assert(tokenLines.contains("A3=[show-access-token]"))
        assert(tokenLines.contains("A5=[SForce (tooling-force.com) SFDX]"))
        assert(tokenLines.contains("A6=[--json]"))
        assert(tokenLines.contains("A7=[--no-prompt]"))
    }

    test("parseDisplayJson reads instanceUrl and ignores accessToken") {
        val token = SfCliAuth.parseDisplayJson(
            """{"status":0,"result":{"accessToken":"[REDACTED] Use 'sf org auth show-access-token' to view","instanceUrl":"https://example.invalid"}}""")
        assert(token == OAuthToken("", "https://example.invalid"))
    }

    test("parseDisplayJson accepts missing accessToken") {
        val display = SfCliAuth.parseDisplayJson(
            """{"status":0,"result":{"instanceUrl":"https://example.invalid"}}""")
        assert(display == OAuthToken("", "https://example.invalid"))
    }

    test("parseTokenJson reads result accessToken") {
        assert(SfCliAuth.parseTokenJson("""{"status":0,"result":{"accessToken":"recorded-access-value"}}""") ==
            OAuthToken("recorded-access-value", ""))
    }

    test("parseTokenJson reports missing result accessToken") {
        val error = intercept[RuntimeException] {
            SfCliAuth.parseTokenJson("""{"status":0,"result":{}}""")
        }
        assert(error.getMessage.contains("org auth show-access-token"))
        assert(error.getMessage.contains("accessToken"))
    }

    test("parseDisplayJson reports a recorded CLI error payload") {
        val error = intercept[RuntimeException] {
            SfCliAuth.parseDisplayJson("""{"status":1,"name":"OrgNotFound","message":"No authorization information found"}""")
        }
        assert(error.getMessage.contains("No authorization information found"))
        assert(error.getMessage.contains("sf org login web --alias"))
    }

    test("parseDisplayJson rejects missing instanceUrl") {
        val error = intercept[RuntimeException] {
            SfCliAuth.parseDisplayJson("""{"status":0,"result":{}}""")
        }
        assert(error.getMessage.contains("instanceUrl"))
    }

    test("non-zero command errors include stderr but not stdout token values") {
        val error = SfCliAuth.commandError("sf", "backup-org", 1, "recorded-access-value", "command failed")
        assert(error.getMessage.contains("command failed"))
        assert(!error.getMessage.contains("recorded-access-value"))
    }

    test("unauthenticated command errors recommend login with the org alias") {
        val error = SfCliAuth.commandError("sf", "backup-org", 1, "", "No authorization information found")
        assert(error.getMessage.contains("sf org login web --alias backup-org"))
    }

    test("missing executable error identifies the configured executable") {
        val error = SfCliAuth.executableError("C:/tools/sf.exe", new java.io.IOException("not found"))
        assert(error.getMessage.contains("C:/tools/sf.exe"))
    }
}
