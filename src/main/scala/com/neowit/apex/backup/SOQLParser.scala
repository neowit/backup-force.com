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

    val twoParts = """select (.*) from (.*)""".r
    val fourParts = """select (.*) from (.*) where (.*) limit (.*)""".r
    val selectLimit = """select (.*)from(.*) limit (.*)""".r
    val selectWhere = """select (.*) from (.*) where (.*)""".r

    private val (selectVal, fromVal, whereVal, limitVal) = soqlStr.toLowerCase match {
        case fourParts(sel, frm, wh, lim) => (sel, frm, wh, lim.trim)
        case selectLimit(sel, frm, lim)  => (sel, frm, None, lim.trim)
        case selectWhere(sel, frm, wh)  => (sel, frm, wh, None)
        case twoParts(sel, frm) => (sel, frm, None, None)
        case _ => (None, None, None, None)
    }
    def select = getAsString(selectVal)
    def from = getAsString(fromVal)
    def where = getAsString(whereVal)
    def limit = getAsString(limitVal)
    def fields:List[String] = if ("*" == select) List("*") else select.split(",").map(fName => fName.trim).toList
    def hasWhere = where.length > 0
    def hasLimit = limit.length > 0
    def isAllFields = fields == List("*")

    private def getAsString(strVal:Any) = strVal match {
        case None => ""
        case str:String => str.trim
    }
}
