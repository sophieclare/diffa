/**
 * Copyright (C) 2010 LShift Ltd.
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
import org.easymock.{IAnswer, EasyMock}
import org.joda.time.DateTime
import net.lshift.diffa.kernel.util.EasyMockScalaUtils._
import net.lshift.diffa.kernel.participants._
import org.apache.commons.codec.digest.DigestUtils
import net.lshift.diffa.kernel.util.Dates._
import net.lshift.diffa.kernel.util.DateUtils._
import net.lshift.diffa.kernel.events._
import org.junit.Test
import collection.mutable.HashMap
import collection.mutable.Map
import net.lshift.diffa.kernel.config.ConfigStore
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.participants.EasyConstraints._

/**
 * Base class for the various policy tests.
 */
abstract class AbstractPolicyTest {
  // The policy instance under test
  protected def policy:VersionPolicy

  // A method for generating a downstream version based on an upstream version
  protected def downstreamVersionFor(v:String):String

  // The various mocks for listeners and participants
  val usMock = createStrictMock("us", classOf[UpstreamParticipant])
  val dsMock = createStrictMock("ds", classOf[DownstreamParticipant])
  val nullListener = new NullDifferencingListener
  
  val store = createStrictMock("versionStore", classOf[VersionCorrelationStore])
  EasyMock.checkOrder(store, false)   // Store doesn't care about order
  val listener = createStrictMock("listener", classOf[DifferencingListener])

  val configStore = createStrictMock("configStore", classOf[ConfigStore])
  val abPair = "A-B"

  val categories = new HashMap[String,String]
  val pair = new net.lshift.diffa.kernel.config.Pair()
  pair.categories = Map("bizDate" -> "date")

  expect(configStore.getPair(abPair)).andReturn(pair).anyTimes

  protected def replayAll = replay(usMock, dsMock, store, listener, configStore)
  protected def verifyAll = verify(usMock, dsMock, store, listener, configStore)

  // Make declaring of sequences of specific types clearer
  def DigestsFromParticipant[T](vals:T*) = Seq[T](vals:_*)
  def VersionsFromStore[T](vals:T*) = Seq[T](vals:_*)

  protected val yearly = YearlyCategoryFunction()
  protected val monthly = MonthlyCategoryFunction()
  protected val daily = DailyCategoryFunction()
  protected val individual = IndividualCategoryFunction()
  
  def Up(v:VersionID, d:DateTime, s:String) = UpstreamVersion(v, Map("bizDate" -> d.toString()), d, s)
  def Down(v:VersionID, d:DateTime, s1:String, s2:String) = DownstreamVersion(v, Map("bizDate" -> d.toString()), d, s1, s2)

  def bizDate(d:DateTime) = Map("bizDate" -> d.toString())

  @Test
  def shouldOnlySyncTopLevelsWhenParticipantsAndStoresMatch {
    // Expect only a top-level sync between the pairs
    //expectUpstreamSync(abPair, DateConstraint(START_2009, END_2010), YearGranularity,
    expectUpstreamAggregateSync(abPair, List(dateRangeConstaint(START_2009, END_2010, yearly)),
      DigestsFromParticipant(
        AggregateDigest(Seq("2009"), null, DigestUtils.md5Hex("vsn1")),
        //Digest("2009", START_2009, null, DigestUtils.md5Hex("vsn1")),
        AggregateDigest(Seq("2010"), null, DigestUtils.md5Hex("vsn2"))),
        //Digest("2010", START_2010, null, DigestUtils.md5Hex("vsn2"))),
      VersionsFromStore(
        Up(VersionID(abPair, "id1"), JUN_6_2009_1, "vsn1"),
        //UpstreamVersion(VersionID(abPair, "id1"), JUN_6_2009_1, JUN_6_2009_1, "vsn1"),
        Up(VersionID(abPair, "id2"), JUL_8_2010_1, "vsn2")))
        //UpstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2")))
    //expectDownstreamSync(abPair, DateConstraint(START_2009, END_2010), YearGranularity,
    expectDownstreamAggregateSync(abPair, List(dateRangeConstaint(START_2009, END_2010, yearly)),
      DigestsFromParticipant(
        AggregateDigest(Seq("2009"),  null, DigestUtils.md5Hex(downstreamVersionFor("vsn1"))),
        //Digest("2009", START_2009, null, DigestUtils.md5Hex(downstreamVersionFor("vsn1"))),
        AggregateDigest(Seq("2010"), null, DigestUtils.md5Hex(downstreamVersionFor("vsn2")))),
        //Digest("2010", START_2010, null, DigestUtils.md5Hex(downstreamVersionFor("vsn2")))),
      VersionsFromStore(
        DownstreamVersion(VersionID(abPair, "id1"), bizDate(JUN_6_2009_1), JUN_6_2009_1, "vsn1", downstreamVersionFor("vsn1")),
        //DownstreamVersion(VersionID(abPair, "id1"), JUN_6_2009_1, JUN_6_2009_1, "vsn1", downstreamVersionFor("vsn1")),
        DownstreamVersion(VersionID(abPair, "id2"), bizDate(JUL_8_2010_1), JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2"))))
        //DownstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2"))))

    // We should still see an unmatched version check
    expect(store.unmatchedVersions(EasyMock.eq(abPair), EasyMock.eq(Seq(dateRangeConstaint(START_2009, END_2010, yearly))))).
        andReturn(Seq())
    replayAll

    policy.difference(abPair, List(dateRangeConstaint(START_2009, END_2010, yearly)), usMock, dsMock, nullListener)
    verifyAll
  }

  @Test
  def shouldUpdateUpstreamVersionsWhenStoreIsOutOfDateWithUpstreamParticipant {
    val timestamp = new DateTime()
    // Expect only a top-level sync between the pairs
    expectUpstreamAggregateSync(abPair, List(dateRangeConstaint(START_2009, END_2010, yearly)),
    //expectUpstreamSync(abPair, DateConstraint(START_2009, END_2010), YearGranularity,
      DigestsFromParticipant(
        AggregateDigest(Seq("2009"), START_2009, DigestUtils.md5Hex("vsn1")),
        //Digest("2009", START_2009, START_2009, DigestUtils.md5Hex("vsn1")),
        AggregateDigest(Seq("2010"), START_2010, DigestUtils.md5Hex("vsn2new" + "vsn4"))),
        //Digest("2010", START_2010, START_2010, DigestUtils.md5Hex("vsn2new" + "vsn4"))),
      VersionsFromStore(
        Up(VersionID(abPair, "id1"), JUN_6_2009_1, "vsn1"),
        //UpstreamVersion(VersionID(abPair, "id1"), JUN_6_2009_1, JUN_6_2009_1, "vsn1"),
        Up(VersionID(abPair, "id2"), JUL_8_2010_1, "vsn2"),
        //UpstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2"),
        Up(VersionID(abPair, "id3"), JUL_8_2010_1, "vsn3")))
        //UpstreamVersion(VersionID(abPair, "id3"), JUL_8_2010_1, JUL_8_2010_1, "vsn3")))
    expectUpstreamAggregateSync(abPair, List(dateRangeConstaint(START_2010, END_2010, monthly)),
    //expectUpstreamSync(abPair, DateConstraint(START_2010, END_2010), MonthGranularity,
      DigestsFromParticipant(
        AggregateDigest(Seq("2010-07"), JUL_8_2010_1, DigestUtils.md5Hex("vsn2new" + "vsn4"))),
        //Digest("2010-07", JUL_8_2010_1, JUL_8_2010_1, DigestUtils.md5Hex("vsn2new" + "vsn4"))),
      VersionsFromStore(
        Up(VersionID(abPair, "id2"), JUL_8_2010_1, "vsn2"),
        //UpstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2"),
        Up(VersionID(abPair, "id3"), JUL_8_2010_1, "vsn3")))
        //UpstreamVersion(VersionID(abPair, "id3"), JUL_8_2010_1, JUL_8_2010_1, "vsn3")))
    expectUpstreamAggregateSync(abPair, List(dateRangeConstaint(JUL_2010, END_JUL_2010, daily)),
    //expectUpstreamSync(abPair, DateConstraint(JUL_2010, END_JUL_2010), DayGranularity,
      DigestsFromParticipant(
        AggregateDigest(Seq("2010-07-08"), JUL_8_2010_1, DigestUtils.md5Hex("vsn2new"  + "vsn4"))),
        //Digest("2010-07-08", JUL_8_2010_1, JUL_8_2010_1, DigestUtils.md5Hex("vsn2new"  + "vsn4"))),
      VersionsFromStore(
        Up(VersionID(abPair, "id2"), JUL_8_2010_1, "vsn2"),
        //UpstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2"),
        Up(VersionID(abPair, "id3"), JUL_8_2010_1, "vsn3")))
        //UpstreamVersion(VersionID(abPair, "id3"), JUL_8_2010_1, JUL_8_2010_1, "vsn3")))
    expectUpstreamEntitySync2(abPair, List(dateRangeConstaint(JUL_8_2010, endOfDay(JUL_8_2010), individual)),
    //expectUpstreamSync(abPair, DateConstraint(JUL_8_2010, endOfDay(JUL_8_2010)), IndividualGranularity,
      DigestsFromParticipant(
        EntityVersion("id2", Seq(JUL_8_2010_1.toString), JUL_8_2010_1, "vsn2new"),
        //Digest("id2", JUL_8_2010_1, JUL_8_2010_1, "vsn2new"),
        EntityVersion("id4", Seq(JUL_8_2010_1.toString), JUL_8_2010_1, "vsn4")),
        //Digest("id4", JUL_8_2010_1, JUL_8_2010_1, "vsn4")),
      VersionsFromStore(
        Up(VersionID(abPair, "id2"), JUL_8_2010_1, "vsn2"),
        //UpstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2"),
        Up(VersionID(abPair, "id3"), JUL_8_2010_1, "vsn3")))
        //UpstreamVersion(VersionID(abPair, "id3"), JUL_8_2010_1, JUL_8_2010_1, "vsn3")))

    expectDownstreamAggregateSync(abPair, List(dateRangeConstaint(START_2009, END_2010, yearly)),
    //expectDownstreamSync(abPair, DateConstraint(START_2009, END_2010), YearGranularity,
      DigestsFromParticipant(
        AggregateDigest(Seq("2009"), START_2009, DigestUtils.md5Hex(downstreamVersionFor("vsn1"))),
        //Digest("2009", START_2009, START_2009, DigestUtils.md5Hex(downstreamVersionFor("vsn1"))),
        AggregateDigest(Seq("2010"), START_2010, DigestUtils.md5Hex(downstreamVersionFor("vsn2")))),
        //Digest("2010", START_2010, START_2010, DigestUtils.md5Hex(downstreamVersionFor("vsn2")))),
      VersionsFromStore(
        Down(VersionID(abPair, "id1"), JUN_6_2009_1, "vsn1", downstreamVersionFor("vsn1")),
        //__DownstreamVersion(VersionID(abPair, "id1"), JUN_6_2009_1, JUN_6_2009_1, "vsn1", downstreamVersionFor("vsn1")),
        Down(VersionID(abPair, "id2"), JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2"))))
        //__DownstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2"))))

    // The policy should update the version for id2, remove id3 and add id4
    //expect(store.storeUpstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2new")).
    expect(store.storeUpstreamVersion(VersionID(abPair, "id2"), bizDate(JUL_8_2010_1), JUL_8_2010_1, "vsn2new")).
      andReturn(Correlation(null, abPair, "id3", bizDate(JUL_8_2010_1), null, JUL_8_2010_1, timestamp, "vsn2new", "vsn2", downstreamVersionFor("vsn2"), false))
      //andReturn(Correlation(null, abPair, "id3", JUL_8_2010_1, JUL_8_2010_1, timestamp, "vsn2new", "vsn2", downstreamVersionFor("vsn2"), false))
    expect(store.clearUpstreamVersion(VersionID(abPair, "id3"))).
      andReturn(Correlation.asDeleted(abPair, "id3", new DateTime))
    expect(store.storeUpstreamVersion(VersionID(abPair, "id4"), bizDate(JUL_8_2010_1), JUL_8_2010_1, "vsn4")).
    //expect(store.storeUpstreamVersion(VersionID(abPair, "id4"), JUL_8_2010_1, JUL_8_2010_1, "vsn4")).
      andReturn(Correlation(null, abPair, "id4", bizDate(JUL_8_2010_1), null, JUL_8_2010_1, timestamp, downstreamVersionFor("vsn2"), null, null, false))
      //andReturn(Correlation(null, abPair, "id4", JUL_8_2010_1, JUL_8_2010_1, timestamp, downstreamVersionFor("vsn2"), null, null, false))

    // Don't report any unmatched versions
    expect(store.unmatchedVersions(EasyMock.eq(abPair), EasyMock.eq(Seq(dateRangeConstaint(START_2009, END_2010, yearly))))).
        andReturn(Seq())
    replayAll

    policy.difference(abPair, List(dateRangeConstaint(START_2009, END_2010, YearlyCategoryFunction())), usMock, dsMock, nullListener)
    verifyAll
  }

  @Test
  def shouldReportMismatchesReportedByUnderlyingStore {
    val timestamp = new DateTime()
    // Expect only a top-level sync between the pairs
    expectUpstreamAggregateSync(abPair, List(dateRangeConstaint(START_2009, END_2010, yearly)),
    //expectUpstreamSync(abPair, DateConstraint(START_2009, END_2010), YearGranularity,
      DigestsFromParticipant(
        AggregateDigest(Seq("2009"), START_2009, DigestUtils.md5Hex("vsn1")),
        //Digest("2009", START_2009, START_2009, DigestUtils.md5Hex("vsn1")),
        AggregateDigest(Seq("2010"), START_2010, DigestUtils.md5Hex("vsn2"))),
        //Digest("2010", START_2010, START_2010, DigestUtils.md5Hex("vsn2"))),
      VersionsFromStore(
        //__UpstreamVersion(VersionID(abPair, "id1"), JUN_6_2009_1, JUN_6_2009_1, "vsn1"),
        Up(VersionID(abPair, "id1"), JUN_6_2009_1, "vsn1"),
        Up(VersionID(abPair, "id2"), JUL_8_2010_1, "vsn2")))
        //__UpstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2")))
    expectDownstreamAggregateSync(abPair, List(dateRangeConstaint(START_2009, END_2010, yearly)),
    //expectDownstreamSync(abPair, DateConstraint(START_2009, END_2010), YearGranularity,
      DigestsFromParticipant(
        AggregateDigest(Seq("2009"), START_2009, DigestUtils.md5Hex(downstreamVersionFor("vsn1a"))),
        //Digest("2009", START_2009, START_2009, DigestUtils.md5Hex(downstreamVersionFor("vsn1a"))),
        AggregateDigest(Seq("2010"), START_2010, DigestUtils.md5Hex(downstreamVersionFor("vsn2a")))),
        //Digest("2010", START_2010, START_2010, DigestUtils.md5Hex(downstreamVersionFor("vsn2a")))),
      VersionsFromStore(
        Down(VersionID(abPair, "id1"), JUN_6_2009_1, "vsn1a", downstreamVersionFor("vsn1a")),
        //__DownstreamVersion(VersionID(abPair, "id1"), JUN_6_2009_1, JUN_6_2009_1, "vsn1a", downstreamVersionFor("vsn1a")),
        Down(VersionID(abPair, "id2"), JUL_8_2010_1, "vsn2a", downstreamVersionFor("vsn2a"))))
        //__DownstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2a", downstreamVersionFor("vsn2a"))))

    // If the version check returns mismatches, we should see differences generated
    expect(store.unmatchedVersions(EasyMock.eq(abPair), EasyMock.eq(Seq(dateRangeConstaint(START_2009, END_2010, yearly))))).
        andReturn(Seq(
          Correlation(null, abPair, "id1", bizDate(JUN_6_2009_1), categories, JUN_6_2009_1, timestamp, "vsn1", "vsn1a", "vsn3", false),
          //Correlation(null, abPair, "id1", JUN_6_2009_1, JUN_6_2009_1, timestamp, "vsn1", "vsn1a", "vsn3", false),
          Correlation(null, abPair, "id2", bizDate(JUL_8_2010_1), categories, JUL_8_2010_1, timestamp, "vsn2", "vsn2a", "vsn4", false)))
          //Correlation(null, abPair, "id2", JUL_8_2010_1, JUL_8_2010_1, timestamp, "vsn2", "vsn2a", "vsn4", false)))
    listener.onMismatch(VersionID(abPair, "id1"), JUN_6_2009_1, "vsn1", "vsn1a"); expectLastCall
    listener.onMismatch(VersionID(abPair, "id2"), JUL_8_2010_1, "vsn2", "vsn2a"); expectLastCall

    replayAll

    policy.difference(abPair, List(dateRangeConstaint(START_2009, END_2010, YearlyCategoryFunction())), usMock, dsMock, listener)
    verifyAll
  }

  @Test
  def shouldStoreUpstreamChangesToCorrelationStoreAndNotifySessionManagerForQuasiLiveDate {
    //val bizDate = JUL_8_2010_1
    val lastUpdate = Some(JUL_8_2010_2)
    storeUpstreamChanges(categories, lastUpdate)
  }

  @Test
  def shouldStoreUpstreamChangesToCorrelationStoreAndNotifySessionManagerWithoutLastUpdate {
    //val bizDate = JUL_8_2010_1
    val lastUpdate = None
    storeUpstreamChanges(categories, lastUpdate)
  }

  /**
   * This is a utility function that allows a kind of virtual date mode for testing
   * historical submissions
   */
  def storeUpstreamChanges(cats:Map[String,String], lastUpdate:Option[DateTime]) {
    val timestamp = new DateTime
    val (update, observationDate, f) = lastUpdate match {
      case None     => (timestamp, null, () =>
        store.storeUpstreamVersion(EasyMock.eq(VersionID(abPair, "id1")), EasyMock.eq(cats),
                                   between(timestamp, timestamp.plusMillis(200)), EasyMock.eq("vsn1")))
      case Some(x)  => (x, x, () => store.storeUpstreamVersion(VersionID(abPair, "id1"), cats, x, "vsn1"))
    }
    expect(f()).andReturn(Correlation(null, abPair, "id1", cats, null, update, timestamp, "vsn1", null, null, false))
    //expect(f()).andReturn(Correlation(null, abPair, "id1", bizDate, update, timestamp, "vsn1", null, null, false))
    listener.onMismatch(VersionID(abPair, "id1"), update, "vsn1", null); expectLastCall
    replayAll

    //policy.onChange(UpstreamPairChangeEvent(VersionID(abPair, "id1"), bizDate, observationDate, "vsn1"))
    policy.onChange(UpstreamPairChangeEvent(VersionID(abPair, "id1"), cats, observationDate, "vsn1"))
    verifyAll
  }

  @Test
  def shouldStoreDownstreamChangesToCorrelationStoreAndNotifySessionManager {
    val timestamp = new DateTime()
    expect(store.storeDownstreamVersion(VersionID(abPair, "id1"), bizDate(JUL_8_2010_2), JUL_8_2010_2, "vsn1", "vsn1")).
    //expect(store.storeDownstreamVersion(VersionID(abPair, "id1"), JUL_8_2010_1, JUL_8_2010_2, "vsn1", "vsn1")).
      andReturn(Correlation(null, abPair, "id1", bizDate(JUL_8_2010_2), categories, JUL_8_2010_2, timestamp, null, "vsn1", "vsn1", false))
      //andReturn(Correlation(null, abPair, "id1", JUL_8_2010_1, JUL_8_2010_2, timestamp, null, "vsn1", "vsn1", false))
    listener.onMismatch(VersionID(abPair, "id1"), JUL_8_2010_2, null, "vsn1"); expectLastCall
    replayAll

    policy.onChange(DownstreamPairChangeEvent(VersionID(abPair, "id1"), bizDate(JUL_8_2010_2), JUL_8_2010_2, "vsn1"))
    //policy.onChange(DownstreamPairChangeEvent(VersionID(abPair, "id1"), JUL_8_2010_1, JUL_8_2010_2, "vsn1"))
    verifyAll
  }

  @Test
  def shouldStoreDownstreamCorrelatedChangesToCorrelationStoreAndNotifySessionManager {
    val timestamp = new DateTime()
    expect(store.storeDownstreamVersion(VersionID(abPair, "id1"), bizDate(JUL_8_2010_2), JUL_8_2010_2, "vsn1", "vsn2")).
    //expect(store.storeDownstreamVersion(VersionID(abPair, "id1"), JUL_8_2010_1, JUL_8_2010_2, "vsn1", "vsn2")).
      andReturn(Correlation(null, abPair, "id1", null, bizDate(JUL_8_2010_2), JUL_8_2010_2, timestamp, null, "vsn1", "vsn1", false))
      //andReturn(Correlation(null, abPair, "id1", JUL_8_2010_1, JUL_8_2010_2, timestamp, null, "vsn1", "vsn1", false))
    listener.onMismatch(VersionID(abPair, "id1"), JUL_8_2010_2, null, "vsn1"); expectLastCall
    replayAll

    policy.onChange(DownstreamCorrelatedPairChangeEvent(VersionID(abPair, "id1"), bizDate(JUL_8_2010_2), JUL_8_2010_2, "vsn1", "vsn2"))
    //policy.onChange(DownstreamCorrelatedPairChangeEvent(VersionID(abPair, "id1"), JUL_8_2010_1, JUL_8_2010_2, "vsn1", "vsn2"))
    verifyAll
  }

  @Test
  def shouldRaiseMatchEventWhenDownstreamCausesMatchOfUpstream {
    val timestamp = new DateTime()
    expect(store.storeDownstreamVersion(VersionID(abPair, "id1"), bizDate(JUL_8_2010_2), JUL_8_2010_2, "vsn1", "vsn2")).
    //expect(store.storeDownstreamVersion(VersionID(abPair, "id1"), JUL_8_2010_1, JUL_8_2010_2, "vsn1", "vsn2")).
      //andReturn(Correlation(null, abPair, "id1", JUL_8_2010_1, JUL_8_2010_2, timestamp, "vsn1", "vsn1", "vsn2", true))
      andReturn(Correlation(null, abPair, "id1", null, bizDate(JUL_8_2010_2), JUL_8_2010_2, timestamp, "vsn1", "vsn1", "vsn2", true))
    listener.onMatch(VersionID(abPair, "id1"), "vsn1"); expectLastCall
    replayAll

    policy.onChange(DownstreamCorrelatedPairChangeEvent(VersionID(abPair, "id1"), bizDate(JUL_8_2010_2), JUL_8_2010_2, "vsn1", "vsn2"))
    //policy.onChange(DownstreamCorrelatedPairChangeEvent(VersionID(abPair, "id1"), JUL_8_2010_1, JUL_8_2010_2, "vsn1", "vsn2"))
    verifyAll
  }


  //
  // Standard Types
  //

  protected case class UpstreamVersion(id:VersionID, categories:Map[String,String], lastUpdate:DateTime, vsn:String)
  protected case class UpstreamVersionAnswer(hs:Seq[UpstreamVersion]) extends IAnswer[Unit] {
    def answer {
      val args = EasyMock.getCurrentArguments
      val cb = args(2).asInstanceOf[Function4[VersionID, Map[String,String], DateTime, String, Unit]]

      hs.foreach { case UpstreamVersion(id, date, lastUpdate, vsn) =>
        cb(id, date, lastUpdate, vsn)
      }
    }
  }
  protected case class DownstreamVersion(id:VersionID, categories:Map[String,String], lastUpdate:DateTime, usvn:String, dsvn:String)
  protected case class DownstreamVersionAnswer(hs:Seq[DownstreamVersion]) extends IAnswer[Unit] {
    def answer {
      val args = EasyMock.getCurrentArguments
      val cb = args(2).asInstanceOf[Function5[VersionID, Map[String,String], DateTime, String, String, Unit]]

      hs.foreach { case DownstreamVersion(id, date, lastUpdate, uvsn, dvsn) =>
        cb(id, date, lastUpdate, uvsn, dvsn)
      }
    }
  }

  protected def expectUpstreamAggregateSync(pair:String, constraints:Seq[QueryConstraint] ,partResp:Seq[AggregateDigest], storeResp:Seq[UpstreamVersion]) {
    expect(usMock.queryAggregateDigests(constraints)).andReturn(partResp)
    store.queryUpstreams(EasyMock.eq(pair), EasyMock.eq(constraints), anyUnitF4)
      expectLastCall[Unit].andAnswer(UpstreamVersionAnswer(storeResp))
  }
  protected def expectDownstreamAggregateSync(pair:String, constraints:Seq[QueryConstraint], partResp:Seq[AggregateDigest], storeResp:Seq[DownstreamVersion]) {
    expect(dsMock.queryAggregateDigests(constraints)).andReturn(partResp)
    store.queryDownstreams(EasyMock.eq(pair), EasyMock.eq(constraints), anyUnitF5)
      expectLastCall[Unit].andAnswer(DownstreamVersionAnswer(storeResp))
  }
  // TODO [#2] these copy and pasted expectations could do with some refactoring
  protected def expectUpstreamEntitySync(pair:String, constraints:Seq[QueryConstraint] ,partResp:Seq[EntityVersion], storeResp:Seq[UpstreamVersion]) {
    expect(usMock.queryEntityVersions(constraints)).andReturn(partResp)
    store.queryUpstreams(EasyMock.eq(pair), EasyMock.eq(constraints), anyUnitF4)
      expectLastCall[Unit].andAnswer(UpstreamVersionAnswer(storeResp))
  }
  protected def expectDownstreamEntitySync(pair:String, constraints:Seq[QueryConstraint], partResp:Seq[EntityVersion], storeResp:Seq[DownstreamVersion]) {
    expect(dsMock.queryEntityVersions(constraints)).andReturn(partResp)
    store.queryDownstreams(EasyMock.eq(pair), EasyMock.eq(constraints), anyUnitF5)
      expectLastCall[Unit].andAnswer(DownstreamVersionAnswer(storeResp))
  }
  protected def expectUpstreamEntitySync2(pair:String, constraints:Seq[QueryConstraint], partResp:Seq[EntityVersion], storeResp:Seq[UpstreamVersion]) {
    expect(usMock.queryEntityVersions(constraints)).andReturn(partResp)
    val correlations = storeResp.map(r => {
      val c = new Correlation()
      c.id = r.id.id
      c.upstreamAttributes = r.categories
      c.lastUpdate = r.lastUpdate
      c.upstreamVsn = r.vsn
      c
    })

    expect(store.queryUpstreams(EasyMock.eq(pair), EasyMock.eq(constraints))).andReturn(correlations)
  }
  protected def expectDownstreamEntitySync2(pair:String, constraints:Seq[QueryConstraint], partResp:Seq[EntityVersion], storeResp:Seq[DownstreamVersion]) {
    expect(dsMock.queryEntityVersions(constraints)).andReturn(partResp)
    val correlations = storeResp.map(r => {
      val c = new Correlation()      
      c.id = r.id.id
      c.downstreamAttributes = r.categories
      c.lastUpdate = r.lastUpdate
      c.downstreamDVsn = r.dsvn
      c
    })

    expect(store.queryDownstreams(EasyMock.eq(pair), EasyMock.eq(constraints))).andReturn(correlations)
  }
}