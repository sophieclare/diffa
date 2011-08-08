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

package net.lshift.diffa.kernel.differencing

import org.easymock.EasyMock._
import net.lshift.diffa.kernel.events.VersionID
import org.junit.{Before, Test}
import org.junit.Assert._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.matching.{MatchingStatusListener, EventMatcher, MatchingManager}
import net.lshift.diffa.kernel.actors.PairPolicyClient
import org.easymock.EasyMock
import net.lshift.diffa.kernel.config.system.SystemConfigStore
import net.lshift.diffa.participant.scanning.ScanConstraint
import net.lshift.diffa.kernel.config.{DiffaPairRef, Domain, Endpoint, DomainConfigStore, Pair => DiffaPair}
import net.lshift.diffa.kernel.frontend.FrontendConversions

/**
 * Test cases for the participant protocol factory.
 */
class StubParticipantProtocolFactory
    extends ScanningParticipantFactory {

  def supportsAddress(address:String, protocol:String) = true

  def createParticipantRef(address: String, protocol: String) = new ScanningParticipantRef {
    def scan(constraints: Seq[ScanConstraint], aggregations: Seq[CategoryFunction]) = null
    def close() {}
  }
}

class DefaultDifferencesManagerTest {
  val cacheProvider = new LocalDomainCacheProvider

  val listener = createStrictMock("listener1", classOf[DifferencingListener])

  val matcher = createStrictMock("matcher", classOf[EventMatcher])

  val domainName = "domain"
  val domainName2 = "domain2"
  val domain1 = Domain(name=domainName)
  val domain2 = Domain(name=domainName2)

  val u = Endpoint(name = "1", scanUrl = "http://foo.com/scan", contentType = "application/json", inboundUrl = "changes", inboundContentType = "application/json")
  val d = Endpoint(name = "2", scanUrl = "http://bar.com/scan", contentType = "application/json", inboundUrl = "changes", inboundContentType = "application/json")

  val pair1 = DiffaPair(key = "pair1", domain = domain1, versionPolicyName = "policy", upstream = u, downstream = d)
  val pair2 = DiffaPair(key = "pair2", domain = domain1, versionPolicyName = "policy", upstream = u, downstream = d, matchingTimeout = 5)


  val systemConfigStore = createStrictMock("systemConfigStore", classOf[SystemConfigStore])
  expect(systemConfigStore.listDomains).andStubReturn(Seq(domain1, domain2))
  replay(systemConfigStore)

  val domainConfigStore = createStrictMock("domainConfigStore", classOf[DomainConfigStore])
  expect(domainConfigStore.listPairs(domainName)).andStubReturn(Seq(FrontendConversions.toPairDef(pair1), FrontendConversions.toPairDef(pair2)))
  replay(domainConfigStore)

  val matchingManager = createStrictMock("matchingManager", classOf[MatchingManager])
  matchingManager.addListener(anyObject.asInstanceOf[MatchingStatusListener]); expectLastCall.once
  replay(matchingManager)

  // TODO The versionPolicyManager and participantFactory should probably go into some
  // kind of test factory - i.e. look out for copy and paste

  val versionPolicy = createStrictMock("vp", classOf[VersionPolicy])


  val versionPolicyManager = new VersionPolicyManager()
  versionPolicyManager.registerPolicy("policy", versionPolicy)
  private val protocol1 = new StubParticipantProtocolFactory()
  private val protocol2 = new StubParticipantProtocolFactory()

  val participantFactory = new ParticipantFactory()
  participantFactory.registerScanningFactory(protocol1)
  participantFactory.registerScanningFactory(protocol2)

  val pairPolicyClient = createStrictMock("pairPolicyClient", classOf[PairPolicyClient])
  EasyMock.checkOrder(pairPolicyClient, false)


  val manager = new DefaultDifferencesManager(
    systemConfigStore, domainConfigStore, cacheProvider, matchingManager,
    versionPolicyManager, pairPolicyClient, participantFactory, listener)

  // The matching manager and config stores will have been called on difference manager startup. Reset them so they
  // can be re-used
  verify(matchingManager, systemConfigStore, domainConfigStore)
  reset(matchingManager, systemConfigStore, domainConfigStore)

  @Before
  def setupStubs = {
    // TODO consider using a stub factory to build stateful objects
    // that always just get copy and pasted
    // i.e. only stub out the behavior that actually care about and want to test
    // so in this example, everything above this comment should be expect() calls
    // and everything below should be stub() calls on a factory
   
    participantFactory.createUpstreamParticipant(u)
    participantFactory.createDownstreamParticipant(d) 

    expect(systemConfigStore.getPair(domainName, "pair1")).andStubReturn(pair1)
    expect(systemConfigStore.getPair(domainName, "pair2")).andStubReturn(pair2)
    expect(systemConfigStore.listPairs).andStubReturn(Seq(pair1,pair2))
    expect(matchingManager.getMatcher(pair1)).andStubReturn(Some(matcher))
    expect(matchingManager.getMatcher(pair2)).andStubReturn(Some(matcher))
    expect(matcher.isVersionIDActive(VersionID(pair1.asRef, "id"))).andStubReturn(true)
    expect(matcher.isVersionIDActive(VersionID(pair2.asRef, "id"))).andStubReturn(true)
    expect(matcher.isVersionIDActive(VersionID(pair1.asRef, "id2"))).andStubReturn(false)

    replay(systemConfigStore, matchingManager)
  }

  def expectDifferenceForPair(pairs:DiffaPair*)  = {
    pairs.foreach(p => {
      expect(pairPolicyClient.difference(p.asRef)).atLeastOnce
    })

    replay(pairPolicyClient)
  }
  def expectScanAndDifferenceForPair(pairs:DiffaPair*)  = {
    pairs.foreach(p => {
      expect(pairPolicyClient.difference(p.asRef)).atLeastOnce
    })
    pairs.foreach(p => {
      expect(pairPolicyClient.scanPair(p.asRef)).atLeastOnce
    })

    replay(pairPolicyClient)
  }

  @Test
 def shouldNotInformMatchEvents {
    // The listener should never be invoked, since listeners can just subscribe directly to the difference output
    replayAll

    expectDifferenceForPair(pair1, pair2)

    manager.onMatch(VersionID(DiffaPairRef(pair1.key,"domain"), "id"), "vsn", LiveWindow)

    verifyAll
  }

  @Test
  def shouldNotTriggerMismatchEventsWhenIdIsActive {
    val timestamp = new DateTime()
    // Replay to get blank stubs
    replayAll

    expectDifferenceForPair(pair1, pair2)

    manager.onMismatch(VersionID(pair1.asRef, "id"), timestamp, "uvsn", "dvsn", LiveWindow, Unfiltered)
    verifyAll
  }

  @Test
  def shouldTriggerMismatchEventsWhenIdIsInactive {
    val timestamp = new DateTime()
    expect(listener.onMismatch(VersionID(DiffaPairRef("pair1", domainName), "id2"), timestamp, "uvsn", "dvsn", LiveWindow, MatcherFiltered))
    replayAll

    expectDifferenceForPair(pair1, pair2)

    manager.onMismatch(VersionID(DiffaPairRef("pair1", domainName), "id2"), timestamp, "uvsn", "dvsn", LiveWindow, Unfiltered)
    verifyAll
  }

  @Test
  def shouldNotGenerateUnmatchedEventWhenANonPendingEventExpires {
    // This will frequently occur when a repair action occurs. A miscellaneous downstream will be seen, which the
    // correlation store will immediately match, but the EventMatcher will see as expiring.

    replayAll

    expectDifferenceForPair(pair1, pair2)

    manager.onDownstreamExpired(VersionID(DiffaPairRef("pair",domainName), "unknownid"), "dvsn")
  }

  @Test
  def shouldHandleExpiryOfAnEventThatIsPending {
    val id = VersionID(pair2.asRef, "id1")
    val now = new DateTime

    expect(matcher.isVersionIDActive(id)).andReturn(true).once()
    listener.onMismatch(id, now, "u", "d", LiveWindow, MatcherFiltered); expectLastCall().once()
    replayAll

    expectDifferenceForPair(pair1, pair2)

    manager.onMismatch(id, now, "u", "d", LiveWindow, Unfiltered)
    manager.onDownstreamExpired(id, "d")
  }

  private def replayAll = replay(listener, matcher)
  private def verifyAll = verify(listener, matcher)
}