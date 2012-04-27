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

import net.lshift.diffa.participant.scanning.ScanConstraint
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.config.system.SystemConfigStore


trait InternalParticipantFactory {

  def supportsAddress(address: String) = {
    address == ("diffa://users") || address == ("diffa://domains")
  }

}

class InternalScanningParticipantFactory(systemConfigStore:SystemConfigStore)
  extends ScanningParticipantFactory with InternalParticipantFactory {

  def createParticipantRef(address: String) = address.replace("diffa://", "") match {
    case "users" => new UsersScanningParticipant(systemConfigStore)
  }
}

class InternalContentParticipantFactory
  extends ContentParticipantFactory with InternalParticipantFactory {

  def createParticipantRef(address: String) = new ContentParticipantRef() {

    def retrieveContent(identifier: String) = null

    def close() {}
  }
}

class InternalVersioningParticipantFactory
  extends VersioningParticipantFactory with InternalParticipantFactory {

  def createParticipantRef(address: String) = new VersioningParticipantRef {

    def generateVersion(entityBody: String) = null

    def close() {}
  }

}
