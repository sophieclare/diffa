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

import org.junit.Assert._
import net.lshift.diffa.kernel.events.VersionID
import org.junit.{Ignore, Test}
import org.joda.time.{DateTime, Interval}
import net.lshift.diffa.kernel.config.DiffaPairRef

/**
 * Test cases for the local domain cache.
 */
class LocalDomainCacheTest {
  val cache = new LocalDomainCache("domain1234")

  @Test
  def shouldMakeDomainAvailable {
    assertEquals("domain1234", cache.domain)
  }

  @Test
  def shouldNotPublishPendingUnmatchedEventInAllUnmatchedList {
    val now = new DateTime()
    cache.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), now, "uV", "dV", now)
    val interval = new Interval(now.minusDays(1), now.plusDays(1))
    assertEquals(0, cache.retrieveUnmatchedEvents(interval).length)
  }

  @Test
  def shouldPublishUpgradedUnmatchedEventInAllUnmatchedList {
    val timestamp = new DateTime()
    cache.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    cache.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    assertEquals(1, unmatched.length)
    assertEquals(VersionID(DiffaPairRef("pair1",  "domain"), "id1"), unmatched.first.objId)
    assertEquals(timestamp, unmatched.first.detectedAt)
    assertEquals("uV", unmatched.first.upstreamVsn)
    assertEquals("dV", unmatched.first.downstreamVsn)
  }

  @Test
  def shouldIgnoreUpgradeRequestsForUnknownIDs {
    cache.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))
    val interval = new Interval(new DateTime(), new DateTime())
    assertEquals(0, cache.retrieveUnmatchedEvents(interval).length)
  }

  @Test
  def shouldIgnoreUpgradeRequestWhenPendingEventHasBeenUpgradedAlready {
    val timestamp = new DateTime()
    cache.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    cache.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))
    cache.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    assertEquals(1, cache.retrieveUnmatchedEvents(interval).length)
  }

  @Test
  def shouldIgnoreUpgradeRequestWhenPendingEventHasBeenCancelled {
    val timestamp = new DateTime()
    cache.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    cache.cancelPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), "uV")
    cache.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    assertEquals(0, cache.retrieveUnmatchedEvents(interval).length)
  }

  @Test
  def shouldNotCancelPendingEventWhenProvidedVersionIsDifferent {
    val timestamp = new DateTime()
    cache.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    cache.cancelPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), "uV-different")
    cache.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    assertEquals(1, cache.retrieveUnmatchedEvents(interval).length)
  }

  @Test
  def shouldPublishAnAddedReportableUnmatchedEvent {
    val timestamp = new DateTime()
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    assertEquals(1, unmatched.length)
    assertEquals(MatchState.UNMATCHED, unmatched.first.state)
    assertEquals(VersionID(DiffaPairRef("pair2",  "domain"), "id2"), unmatched.first.objId)
    assertEquals(timestamp, unmatched.first.detectedAt)
    assertEquals("uV", unmatched.first.upstreamVsn)
    assertEquals("dV", unmatched.first.downstreamVsn)
  }

  @Test
  def shouldReportUnmatchedEventWithinInterval = {
    val start = new DateTime(2004, 11, 6, 3, 5, 15, 0)
    val size = 60
    var frontFence = 10
    var rearFence = 10

    val interval = addUnmatchedEvents(start, size, frontFence, rearFence)

    val unmatched = cache.retrieveUnmatchedEvents(interval)
    assertEquals(size - frontFence - rearFence, unmatched.length)
  }

  @Test
  def shouldCountUnmatchedEventWithinInterval = {
    val start = new DateTime(2004, 11, 6, 3, 5, 15, 0)
    val size = 60
    var frontFence = 10
    var rearFence = 10

    val interval = addUnmatchedEvents(start, size, frontFence, rearFence)

    val unmatchedCount = cache.countEvents("pair2", interval)
    assertEquals(size - frontFence - rearFence, unmatchedCount)
  }

  def addUnmatchedEvents(start:DateTime, size:Int, frontFence:Int, rearFence:Int) : Interval = {
    for (i <- 1 to size) {
      val timestamp = start.plusMinutes(i)
      cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id" + i), timestamp, "uV", "dV", timestamp)
    }
    new Interval(start.plusMinutes(frontFence), start.plusMinutes(size - rearFence))
  }

  @Test
  def shouldPageReportableUnmatchedEvent = {
    val start = new DateTime(1982, 5, 5, 14, 15, 19, 0)
    val size = 100
    var frontFence = 20
    var rearFence = 50

    // Set a bound so that 30 events fall into the window
    val interval = addUnmatchedEvents(start, size, frontFence, rearFence)

    // Create an interval that is wide enough to get every event ever
    val veryWideInterval = new Interval(start.minusDays(1), start.plusDays(1))

    val unmatched = cache.retrieveUnmatchedEvents(veryWideInterval)
    assertEquals(size, unmatched.length)

    // Requesting 19 elements with an offset of 10 from 30 elements should yield elements 10 through to 28
    val containedPage = cache.retrievePagedEvents("pair2", interval, 10, 19)
    assertEquals(19, containedPage.length)

    // Requesting 19 elements with an offset of 20 from 30 elements should yield elements 20 through to 29
    val splitPage = cache.retrievePagedEvents("pair2", interval, 20, 19)
    assertEquals(10, splitPage.length)

  }

  @Test
  def shouldAddMatchedEventThatOverridesUnmatchedEventWhenAskingForSequenceUpdate {
    val timestamp = new DateTime()
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uuV", "ddV", timestamp)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    val lastSeq = unmatched.last.seqId

    cache.addMatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), "uuV")
    val updates = cache.retrieveEventsSince(lastSeq)

    assertEquals(1, updates.length)
    assertEquals(MatchState.MATCHED, updates.first.state)
    // We don't know deterministically when the updated timestamp will be because this
    // is timestamped on the fly from within the implementation of the cache
    // but we do want to assert that it is not before the reporting timestamp
    assertFalse(timestamp.isAfter(updates.first.detectedAt))
    assertEquals(VersionID(DiffaPairRef("pair2", "domain"), "id2"), updates.first.objId)
  }

  @Test
  def shouldRemoveUnmatchedEventFromAllUnmatchedWhenAMatchHasBeenAdded {
    val timestamp = new DateTime()
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uuV", "ddV", timestamp)
    cache.addMatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), "uuV")
    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val updates = cache.retrieveUnmatchedEvents(interval)

    assertEquals(0, updates.length)
  }

  @Test
  def shouldIgnoreMatchedEventWhenNoOverridableUnmatchedEventIsStored {
    val timestamp = new DateTime()
    // Get an initial event and a sequence number
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)
    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    val lastSeq = unmatched.last.seqId

    // Add a matched event for something that we don't have marked as unmatched
    cache.addMatchedEvent(VersionID(DiffaPairRef("pair3","domain"), "id3"), "eV")
    val updates = cache.retrieveEventsSince(lastSeq)
    assertEquals(0, updates.length)
  }

  @Test
  def shouldOverrideOlderUnmatchedEventsWhenNewMismatchesOccurWithDifferentDetails {
    // Add two events for the same object, and then ensure the old list only includes the most recent one
    val timestamp = new DateTime()
    val seen = new DateTime().plusSeconds(5)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV", "dV", timestamp)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV2", "dV2", seen)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    assertEquals(1, unmatched.length)
    assertEquals(VersionID(DiffaPairRef("pair2","domain"), "id2"), unmatched(0).objId)
    assertEquals("uV2", unmatched(0).upstreamVsn)
    assertEquals("dV2", unmatched(0).downstreamVsn)
    assertEquals(seen, unmatched(0).lastSeen)
  }

  @Test
  def shouldRetainOlderUnmatchedEventsWhenNewEventsAreAddedWithSameDetailsButUpdateTheSeenTime {
    // Add two events for the same object with all the same details, and ensure that we don't modify the event
    val timestamp = new DateTime()
    val seen = new DateTime().plusSeconds(5)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV", "dV", timestamp)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp.plusSeconds(15), "uV", "dV", seen)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    assertEquals(1, unmatched.length)
    assertEquals(VersionID(DiffaPairRef("pair2","domain"), "id2"), unmatched(0).objId)
    assertEquals("uV", unmatched(0).upstreamVsn)
    assertEquals("dV", unmatched(0).downstreamVsn)
    assertEquals(timestamp, unmatched(0).detectedAt)
    assertEquals(seen, unmatched(0).lastSeen)
  }

  @Test
  def shouldRemoveEventsNotSeenBeforeTheGivenCutoff {
    val timestamp = new DateTime()
    val seen1 = timestamp.plusSeconds(5)
    val seen2 = timestamp.plusSeconds(8)
    val cutoff = timestamp.plusSeconds(9)
    val seen3 = timestamp.plusSeconds(10)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), timestamp, "uV", "dV", seen1)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV", "dV", seen2)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id3"), timestamp, "uV", "dV", seen3)
    cache.matchEventsOlderThan(cutoff)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    assertEquals(1, unmatched.length)
    assertEquals(VersionID(DiffaPairRef("pair2","domain"), "id3"), unmatched(0).objId)
    assertEquals("uV", unmatched(0).upstreamVsn)
    assertEquals("dV", unmatched(0).downstreamVsn)
    assertEquals(timestamp, unmatched(0).detectedAt)
  }

  @Test
  def shouldAddMatchEventsForThoseRemovedByACutoff {
    val now = new DateTime
    val timestamp = now .minusSeconds(10)
    val seen1 = now .plusSeconds(5)
    val seen2 = now .plusSeconds(8)
    val cutoff = now .plusSeconds(9)
    val seen3 = now .plusSeconds(10)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), timestamp, "uV", "dV", seen1)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV", "dV", seen2)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id3"), timestamp, "uV", "dV", seen3)
    cache.matchEventsOlderThan(cutoff)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveEventsSince("0")
    assertEquals(3, unmatched.length)

    assertEquals(VersionID(DiffaPairRef("pair2","domain"), "id3"), unmatched(0).objId)
    assertEquals("uV", unmatched(0).upstreamVsn)
    assertEquals("dV", unmatched(0).downstreamVsn)
    assertEquals(timestamp, unmatched(0).detectedAt)

    assertEquals(VersionID(DiffaPairRef("pair2","domain"), "id1"), unmatched(1).objId)
    assertEquals("uV", unmatched(1).upstreamVsn)
    assertEquals("uV", unmatched(1).downstreamVsn)
    assertTrue(!unmatched(1).detectedAt.isBefore(now))      // Detection should be some time at or after now
    assertTrue(!unmatched(1).lastSeen.isBefore(now))        // Last seen should be some time at or after now

    assertEquals(VersionID(DiffaPairRef("pair2","domain"), "id2"), unmatched(2).objId)
    assertEquals("uV", unmatched(2).upstreamVsn)
    assertEquals("uV", unmatched(2).downstreamVsn)
    assertTrue(!unmatched(2).detectedAt.isBefore(now))      // Detection should be some time at or after now
    assertTrue(!unmatched(2).lastSeen.isBefore(now))        // Last seen should be some time at or after now
  }
}