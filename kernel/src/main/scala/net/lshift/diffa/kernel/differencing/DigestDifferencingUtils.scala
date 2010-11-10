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

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import collection.mutable.{ListBuffer, HashMap}
import net.lshift.diffa.kernel.participants._
import scala.collection.Map

/**
 * Utility methods for differencing sequences of digests.
 */
object DigestDifferencingUtils {

  def differenceEntities(ds1:Seq[EntityVersion],
                         ds2:Seq[EntityVersion],
                         resolve:Digest => Map[String,String],
                         constraints:Seq[QueryConstraint]) : Seq[VersionMismatch] = {
    val result = new ListBuffer[VersionMismatch]
    val ds1Ids = indexById(ds1)
    val ds2Ids = indexById(ds2)

    ds1Ids.foreach { case (label, ds1Digest) => {
      val (otherMatches, otherDigest, otherDigestUpdated) = ds2Ids.remove(label) match {
        case Some(hs2Digest) => (ds1Digest.digest == hs2Digest.digest, hs2Digest.digest, hs2Digest.lastUpdated)
        case None => (false, null, null)
      }

      if (!otherMatches) {
        result += VersionMismatch(label, resolve(ds1Digest), ds1Digest.lastUpdated, ds1Digest.digest, otherDigest)
      }
    }}

    ds2Ids.foreach { case (label, hs2Digest) => {
      val otherMatches = ds1Ids.remove(label) match {
        case None => false
        case _    => true    // No need to compare, since we did that above
      }

      if (!otherMatches) {
        result += VersionMismatch(label, resolve(hs2Digest), hs2Digest.lastUpdated, null, hs2Digest.digest)
      }
    }}

    result
  }

  def differenceAggregates(ds1:Seq[AggregateDigest],
                           ds2:Seq[AggregateDigest],
                           resolve:Digest => Map[String,String],
                           constraints:Seq[QueryConstraint]) : Seq[QueryAction] = {
    val result = new ListBuffer[QueryAction]
    val ds1Ids = indexByAttributeValues(ds1)
    val ds2Ids = indexByAttributeValues(ds2)

    //val attributes:HashMap[String,String] = null

    ds1Ids.foreach { case (label, ds1Digest) => {
      val (otherMatches, otherDigest, otherDigestUpdated) = ds2Ids.remove(label) match {
        case Some(hs2Digest) => (ds1Digest.digest == hs2Digest.digest, hs2Digest.digest, hs2Digest.lastUpdated)
        case None => (false, null, null)
      }

      if (!otherMatches) {
//        gran match {
//          case IndividualGranularity =>
//            result += VersionMismatch(label, attributes, latestOf(ds1Digest.lastUpdated, otherDigestUpdated), ds1Digest.digest, otherDigest)
//          case _ => otherDigest match {
//            case null => result += deepestQueryAction(label, gran)
//            case _ => result += deeperQueryAction(label, gran)
//          }
//        }
      }
    }}

    ds2Ids.foreach { case (label, hs2Digest) => {
      val otherMatches = ds1Ids.remove(label) match {
        case None => false
        case _    => true    // No need to compare, since we did that above
      }

      if (!otherMatches) {
//        result += VersionMismatch(label, resolve(hs2Digest), hs2Digest.lastUpdated, null, hs2Digest.digest)  
//        gran match {
//          case IndividualGranularity => result += VersionMismatch(label, attributes, hs2Digest.lastUpdated, null, hs2Digest.digest)
//          case _                     => result += deepestQueryAction(label, gran)
//        }
      }
    }}

    result
  }

  private def indexById(hs:Seq[EntityVersion]) = {
    val res = new HashMap[String, EntityVersion]
    hs.foreach(d => res(d.id) = d)
    res
  }
  private def indexByAttributeValues(hs:Seq[Digest]) = {
    val res = new HashMap[String, Digest]
    hs.foreach(d => res(d.attributes.reduceLeft(_+_)) = d)
    res
  }
  private def deepestQueryAction(key:String, currentGran:RangeGranularity) = {
    val (start, end) = dateRangeForKey(key, currentGran)
    EntityQueryAction(DateRangeConstraint(null,null, DateCategoryFunction()))
    //QueryAction(start, end, IndividualGranularity)
  }
  private def deeperQueryAction(key:String, currentGran:RangeGranularity) = {
    val (start, end) = dateRangeForKey(key, currentGran)
    AggregateQueryAction(DateRangeConstraint(null,null,DateCategoryFunction()))
//    QueryAction(start, end, currentGran match {
//      case YearGranularity => MonthGranularity
//      case MonthGranularity => DayGranularity
//      case DayGranularity => IndividualGranularity
//    })
  }

  private val yearParser = DateTimeFormat.forPattern("yyyy")
  private val yearMonthParser = DateTimeFormat.forPattern("yyyy-MM")
  private val yearMonthDayParser = DateTimeFormat.forPattern("yyyy-MM-dd")
  private def dateRangeForKey(key:String, gran:RangeGranularity) = {
    val (startDay, endDay) = gran match {
      case YearGranularity => {
        val point = yearParser.parseDateTime(key).toLocalDate
        (point.withMonthOfYear(1).withDayOfMonth(1), point.withMonthOfYear(12).withDayOfMonth(31))
      }
      case MonthGranularity => {
        val point = yearMonthParser.parseDateTime(key).toLocalDate
        (point.withDayOfMonth(1), point.plusMonths(1).minusDays(1))
      }
      case DayGranularity => {
        val point = yearMonthDayParser.parseDateTime(key).toLocalDate
        (point, point)
      }
    }

    (startDay.toDateTimeAtStartOfDay, endDay.toDateTimeAtStartOfDay.plusDays(1).minusMillis(1))
  }

  private def latestOf(d1:DateTime, d2:DateTime) = {
    (d1, d2) match {
      case (null, null) => new DateTime
      case (_, null)    => d1
      case (null, _)    => d2
      case _            => if (d1.isAfter(d2)) d1 else d2
    }
  }
}

abstract class DifferenceOutcome
abstract class QueryAction extends DifferenceOutcome
case class AggregateQueryAction(constraint:RangeQueryConstraint) extends QueryAction
case class EntityQueryAction(constraint:RangeQueryConstraint) extends QueryAction
case class VersionMismatch(id:String, attributes:Map[String,String], lastUpdated:DateTime, vsnA:String, vsnB:String) extends DifferenceOutcome