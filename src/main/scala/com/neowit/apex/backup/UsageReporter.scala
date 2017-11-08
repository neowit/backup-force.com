/*
 * Copyright (c) 2017 Andrey Gavrikov.
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

import java.io.OutputStreamWriter
import java.security.MessageDigest
import java.net._
import javax.net.ssl.HttpsURLConnection

import com.typesafe.scalalogging.slf4j.LazyLogging
/**
  * this class sends anonymised usage statistics
  * @param config - application config
  */
class UsageReporter(config: Config) extends LazyLogging {

    def report(): Unit = {
        if ("0" != config.getProperty("reportUsage").getOrElse("1")) {
            try {
                reportUsage()
            } catch {
                case x: Throwable => //usage reports are not important enough to do anything about them
                    logger.trace("", x)
            }
        }
    }

    private def reportUsage(): Unit = {
        val localAddress = InetAddress.getLocalHost
        val ni = NetworkInterface.getByInetAddress( localAddress )
        val hardwareAddress = ni.getHardwareAddress
        //get hash of mac address
        val macHash = if (null != hardwareAddress) {
            val mac = hardwareAddress.map("%02X" format _).mkString("-")
            val md5 = MessageDigest.getInstance("MD5")
            md5.digest(mac.getBytes).mkString
        } else {
            "unknown"
        }

        val proxyType = config.getProperty("http.proxyHost") match {
            case Some(x) => config.getProperty("http.proxyUsername") match {
                case Some(y) => "Authenticated"
                case None => "Un-authenticated"
            }
            case None => "None"
        }

        val data = Map(
            "application" -> AppVersion.APP_NAME,
            "action" -> "backup",
            "macHash" -> macHash,
            "os" -> System.getProperty("os.name"),
            "version" -> AppVersion.VERSION,
            "proxyType" -> proxyType
        )
        logger.trace("usage report: " + data)

        UsageReporter.initialiseProxy(config) // this must be done before URL is created
        val url = new URL("https://usage-developer-edition.eu9.force.com/services/apexrest/usage")
        UsageReporter.sendPost(url, data)

    }

}
object UsageReporter extends LazyLogging {
    class ProxyAuthenticator(user: String, password: String) extends Authenticator {
        override def getPasswordAuthentication: PasswordAuthentication = {
            new PasswordAuthentication(user, password.toCharArray)
        }
    }
    private def encodePostData(data: Map[String, String]): String = {
        val keyValueStrings =
            data.map{
                case (key, value) => s""" "$key": "$value" """
            }.mkString(",")
        "{" + keyValueStrings + "}"
    }

    private def initialiseProxy(config: Config): Unit = {
        config.getProperty("http.proxyHost").foreach  {proxyHost =>
            for {
                proxyUser <- config.getProperty("http.proxyUser")
                proxyPassword <- config.getProperty("http.proxyPassword")
            } {
                Authenticator.setDefault(new ProxyAuthenticator(proxyUser, proxyPassword))
            }
            System.setProperty("http.proxyHost", proxyHost)
            config.getProperty("http.proxyPort").foreach(portStr => System.setProperty("http.proxyPort", portStr) )
        }
    }
    private def sendPost(url: URL, payload: Map[String, String]): Unit = {
        val con = url.openConnection.asInstanceOf[HttpsURLConnection]
        con.setRequestProperty("Content-Type", "application/json")
        con.setRequestProperty("User-Agent", s"${AppVersion.APP_NAME}/${AppVersion.VERSION}")
        con.setRequestMethod("POST")
        con.setDoOutput(true)
        con.setDoInput(true)
        // Send post request
        con.connect()
        val wr = new OutputStreamWriter(con.getOutputStream)
        wr.write(encodePostData(payload))
        wr.flush()
        wr.close()
        val in = con.getInputStream
        logger.trace("usage service responded: " + scala.io.Source.fromInputStream( in ).mkString(""))
        in.close()
    }

}
