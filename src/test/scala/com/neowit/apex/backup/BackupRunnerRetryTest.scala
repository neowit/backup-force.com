package com.neowit.apex.backup

import org.scalatest.funsuite.AnyFunSuite

class BackupRunnerRetryTest extends AnyFunSuite {
    test("retries one operation once after INVALID_SESSION_ID") {
        var attempts = 0
        var tokenFetches = 0
        val result = BackupRunner.withSessionRetry(() => { tokenFetches += 1; tokenFetches }) { _ =>
            attempts += 1
            if (attempts == 1) throw new com.sforce.ws.ConnectionException("INVALID_SESSION_ID")
            "complete"
        }
        assert(result == "complete")
        assert(attempts == 2)
        assert(tokenFetches == 2)
    }

    test("does not retry another connection error") {
        var attempts = 0
        intercept[com.sforce.ws.ConnectionException] {
            BackupRunner.withSessionRetry(() => "token") { _ =>
                attempts += 1
                throw new com.sforce.ws.ConnectionException("OTHER_ERROR")
            }
        }
        assert(attempts == 1)
    }

    test("propagates the second INVALID_SESSION_ID failure without a third attempt") {
        var attempts = 0
        var tokenFetches = 0
        intercept[com.sforce.ws.ConnectionException] {
            BackupRunner.withSessionRetry(() => { tokenFetches += 1; tokenFetches }) { _ =>
                attempts += 1
                throw new com.sforce.ws.ConnectionException("INVALID_SESSION_ID")
            }
        }
        assert(attempts == 2)
        assert(tokenFetches == 2)
    }
}
