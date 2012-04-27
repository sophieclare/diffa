/**
 * Copyright (C) 2010-2012 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.lshift.diffa.kernel.participants.internal

import net.lshift.diffa.kernel.participants.{CategoryFunction, ScanningParticipantRef}
import net.lshift.diffa.kernel.config.system.SystemConfigStore
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.config.User
import java.security.MessageDigest
import org.apache.commons.codec.binary.Hex
import net.lshift.diffa.participant.scanning._

/**
 * Performs aggregations on the USERS table
 */
class UsersScanningParticipant(systemConfigStore:SystemConfigStore) extends ScanningParticipantRef {

  def scan(constraints: Seq[ScanConstraint], aggregations: Seq[CategoryFunction]) = {

    val users = systemConfigStore.listUsers

    val filteredUsers = if (constraints == null) {
      users
    } else {
      // For now we can only constrain by username when using set or prefix constraints

      // Also, we should propagate this to the DB instead of doing it here
      val applicableConstraints = constraints.filter(c => c.getAttributeName == "name")

      users.filter(u => {
        applicableConstraints.forall {
          case set:SetConstraint             => set.contains(u.name)
          case prefix:StringPrefixConstraint => prefix.contains(u.name)
          case _                             => false
        }
      })
    }

    val scanResults = filteredUsers.map { u => new ScanResultEntry(u.name, generateVersion(u), null, Map("name" -> u.name)) }

    if (aggregations.length > 0) {
      val digester = new DigestBuilder(aggregations)
      scanResults.foreach(digester.add(_))
      digester.toDigests
    } else {
      scanResults
    }
  }

  // No resources to free
  def close() = {}

  private def generateVersion(user:User) = {
    val digest = MessageDigest.getInstance("MD5")
    def addIfNotNull(field:String) = if (field != null) {
      digest.update(field.getBytes("UTF-8"))
    }

    addIfNotNull(user.name)
    addIfNotNull(user.token)

    new String(Hex.encodeHex(digest.digest()))
  }
}
