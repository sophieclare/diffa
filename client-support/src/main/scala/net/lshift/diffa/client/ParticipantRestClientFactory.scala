/**
 * Copyright (C) 2010-2011 LShift Ltd.
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

package net.lshift.diffa.client

import net.lshift.diffa.kernel.participants._

trait ParticipantRestClientFactory {

  // TODO This should support https as well
  def supportsAddress(address: String) = address.startsWith("http://")
}

// TODO Why do we need the RestClientParams? Should the URI not be sufficient?
class ScanningParticipantRestClientFactory(params: RestClientParams)
  extends ScanningParticipantFactory with ParticipantRestClientFactory {

  def createParticipantRef(address: String) = new ScanningParticipantRestClient(address, params)
}

class ContentParticipantRestClientFactory(params: RestClientParams)
  extends ContentParticipantFactory with ParticipantRestClientFactory {

  def createParticipantRef(address: String) = new ContentParticipantRestClient(address, params)
}

class VersioningParticipantRestClientFactory(params: RestClientParams)
  extends VersioningParticipantFactory with ParticipantRestClientFactory {

  def createParticipantRef(address: String) = new VersioningParticipantRestClient(address, params)
}