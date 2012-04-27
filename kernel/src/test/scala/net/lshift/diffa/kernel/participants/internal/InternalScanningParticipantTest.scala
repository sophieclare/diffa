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

import org.easymock.EasyMock._
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.config.system.SystemConfigStore
import net.lshift.diffa.kernel.config.User
import net.lshift.diffa.kernel.participants.internal.InternalScanningParticipantTest.Scenario
import org.junit.runner.RunWith
import org.junit.Assert._
import org.junit.experimental.theories.{DataPoint, Theories, Theory}
import net.lshift.diffa.participant.scanning.ScanResultEntry
import net.lshift.diffa.kernel.participants.{CategoryFunction, StringPrefixCategoryFunction}

@RunWith(classOf[Theories])
class InternalScanningParticipantTest {



  val systemConfigStore = createStrictMock("systemConfigStore", classOf[SystemConfigStore])
  val participant = new UsersScanningParticipant(systemConfigStore)


  @Theory
  def shouldAggregateUsers(scenario:Scenario) {

    expect(systemConfigStore.listUsers).andReturn(scenario.users).once()
    replay(systemConfigStore)

    assertEquals(scenario.results, participant.scan(null, scenario.bucketing))

    verify(systemConfigStore)
  }
}

object InternalScanningParticipantTest {

  case class Scenario(users:Seq[User], bucketing:Seq[CategoryFunction], results:Seq[ScanResultEntry])

  // TODO The digest for this data point was hardcoded ....
  @DataPoint def singleUser = Scenario(
    Seq(User("abc")),
    Seq(StringPrefixCategoryFunction("name", 1, 3, 1)),
    Seq(ScanResultEntry.forAggregate("ec0405c5aef93e771cd80e0db180b88b", Map("name" -> "a")))
  )

}
