package com.neowit.apex.backup

import junit.framework._
import Assert._

object SOQLParserTest {
    def suite: Test = {
        val suite = new TestSuite(classOf[SOQLParserTest])
        suite
    }

    def main(args : Array[String]) {
        junit.textui.TestRunner.run(suite)
    }
}

/**
 * Unit test for simple App.
 */
class SOQLParserTest extends TestCase("SOQLParser") {

    def testSelectFrom() {
        val p1 = new SOQLParser("select Id, Name from Account")
        assertEquals("id, name", p1.select)
        assertEquals("account", p1.from)
        assertEquals(false, p1.hasTail)
    }
    def testSelectFromWhere() {
        val p0 = new SOQLParser("select Id, Name from Account ")
        assertEquals(false, p0.hasTail)

        val p1 = new SOQLParser("select Id, Name from Account where X > 1 and yyy <= 5 ")
        assertEquals("id, name", p1.select)
        assertEquals("account", p1.from)
        assertEquals(true, p1.hasTail)
    }

    def testSelectFromLimit() {
        val p1 = new SOQLParser("select Id, Name from Account limit 100")
        assertEquals("id, name", p1.select)
        assertEquals("account", p1.from)
        assertEquals(true, p1.hasTail)
    }

}
