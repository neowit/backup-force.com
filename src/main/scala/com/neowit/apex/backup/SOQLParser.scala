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

import akka.pattern.ask
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import akka.util.Timeout


class SOQLParser(soqlStr:String) {

    private val twoParts = """select (.*) from (\w*)""".r
    private val threeParts = """select (.*) from (\w*)(.*)""".r

    private val (selectVal, fromVal, tailVal) = soqlStr match {
        case twoParts(sel, frm) => (sel, frm, None)
        case threeParts(sel, frm, rest) => (sel, frm, rest)
        case _ => (None, None, None)
    }
    lazy val select = getAsString(selectVal)
    lazy val from = getAsString(fromVal)
    lazy val tail = {
        var tailStr = getAsString(tailVal)
        //find "$object.lastmodifieddate"
        val p = "(?i)\\$object\\.lastmodifieddate".r
        //p.replaceAllIn(tailStr, Config.getStoredLastModifiedDate(from))
        val f = Config.lastQueryPropsActor.?("get", from)(Timeout.intToTimeout(1000))
        val lastModifiedDate = Await.result(f, Duration.Inf)
        p.replaceAllIn(tailStr, lastModifiedDate.toString)
    }
    lazy val fields:List[String] = if ("*" == select) List("*") else select.split(",").map(fName => fName.trim).toList
    lazy val hasTail = tail.length > 0
    lazy val isAllFields = fields == List("*")
    //if query contains IsDeleted then this is a hint that user wants ALL Rows (i.e. queryAll)
    lazy val isAllRows = tail.toLowerCase.contains(" isdeleted=") || tail.toLowerCase.contains(" isdeleted ")
    lazy val hasRelationshipFields = select.indexOf(".") >= 0

    private def getAsString(strVal:Any) = strVal match {
        case None => ""
        case str:String => str.trim
    }
}
