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

import com.typesafe.scalalogging.LazyLogging

object Benchmark extends LazyLogging {
    def apply[T](name: String)(block: => T) {
        val start = System.currentTimeMillis
        try {
            block
        } finally {
            val diff = System.currentTimeMillis - start
            logger.debug("# Block \"" + name +"\" completed, time taken: " + diff + " ms (" + diff / 1000.0 + " s)")
        }
    }
}
/* Use it like this
Benchmark("implicit conversion") {
    for (i <- 1 to 100000) {
        fieldList.map(fName => (record.getFieldIgnoreCase(fName)))
    }
}
*/
