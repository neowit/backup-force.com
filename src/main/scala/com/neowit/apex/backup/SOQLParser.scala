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


class SOQLParser(soqlStr:String) {

    val twoParts = """select (.*) from (\w*)""".r
    val threeParts = """select (.*) from (\w*)(.*)""".r

    private val (selectVal, fromVal, tailVal) = soqlStr match {
        case twoParts(sel, frm) => (sel, frm, None)
        case threeParts(sel, frm, rest) => (sel, frm, rest)
        case _ => (None, None, None)
    }
    def select = getAsString(selectVal)
    def from = getAsString(fromVal)
    def tail = {
        var tailStr = getAsString(tailVal)
        //val token = "$object.lastmodifieddate"
        val p = "(?i)\\$object\\.lastmodifieddate".r
        p.replaceAllIn(tailStr, Config.getStoredLastModifiedDate(from))
    }
    def fields:List[String] = if ("*" == select) List("*") else select.split(",").map(fName => fName.trim).toList
    def hasTail = tail.length > 0
    def isAllFields = fields == List("*")

    private def getAsString(strVal:Any) = strVal match {
        case None => ""
        case str:String => str.trim
    }
}
