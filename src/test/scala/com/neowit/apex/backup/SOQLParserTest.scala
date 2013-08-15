package com.neowit.apex.backup


import org.scalatest.{PrivateMethodTester, FunSuite}
import java.util.Properties

class SOQLParserTest extends FunSuite with PrivateMethodTester {

    test("Select From") {
        val p1 = new SOQLParser("select Id, Name from Account")
        assert("Id, Name" == p1.select)
        assert("Account" == p1.from)
        assert(false == p1.hasTail)
    }
    test("Select From Where") {
        val p0 = new SOQLParser("select Id, Name from Account ")
        assert(false == p0.hasTail)

        val p1 = new SOQLParser("select Id, Name from Account where X > 1 and yyy <= 5 ")
        assert("Id, Name" == p1.select)
        assert("Account" == p1.from)
        assert(true == p1.hasTail)
    }

    test("Select From Limit") {
        val p1 = new SOQLParser("select Id, Name from Account limit 100")
        assert("Id, Name" == p1.select)
        assert("Account" == p1.from)
        assert(true == p1.hasTail)
    }

    test("LastModifiedDate Replacement") {
        val lastQueryProps = PrivateMethod[Properties]('lastQueryProps)
        val props = Config invokePrivate lastQueryProps()
        props.setProperty("account", "1917-01-01T00:00:00Z")
        val soql1 = "select Id, name from Account where LastModifiedDate >= $Object.Lastmodifieddate"
        assert("where LastModifiedDate >= 1917-01-01T00:00:00Z" == new SOQLParser(soql1).tail)

        val soql2 = "select Id, name from Account where LastModifieddate >= $Object.Lastmodifieddate and F__c > 0 and createdDate <= $object.lastModifiedDate "
        assert("where LastModifieddate >= 1917-01-01T00:00:00Z and F__c > 0 and createdDate <= 1917-01-01T00:00:00Z" == new SOQLParser(soql2).tail)

    }

    test("Query & QueryAll") {
        val p1 = new SOQLParser("select Id, Name from Account limit 100")
        assert(!p1.isAllRows)

        val p2 = new SOQLParser("select Id, Name from Account where Id > '' limit 100")
        assert(!p2.isAllRows)

        val p3 = new SOQLParser("select Id, Name from Account where Id > '' and IsDeleted=true limit 100")
        assert(p3.isAllRows)

        val p4 = new SOQLParser("select Id, Name from Account where Id > '' and IsDeleted  = true limit 100")
        assert(p4.isAllRows)
    }

}
