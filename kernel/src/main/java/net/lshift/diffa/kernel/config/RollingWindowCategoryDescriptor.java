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

package net.lshift.diffa.kernel.config;

import net.lshift.diffa.kernel.util.InvalidConstraintException;
import net.lshift.diffa.participant.scanning.DateRangeConstraint;
import net.lshift.diffa.participant.scanning.RangeConstraint;
import net.lshift.diffa.participant.scanning.ScanConstraint;

import net.lshift.diffa.participant.scanning.TimeRangeConstraint;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.ISOPeriodFormat;
import org.joda.time.format.PeriodFormatter;

/**
 * A category that is constrained by a moving window that behaves as a dynamically computed range category.
 */
public class RollingWindowCategoryDescriptor extends CategoryDescriptor {
  private static final PeriodFormatter periodFormatter = ISOPeriodFormat.standard();
  private static final DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTime();

  private final boolean isOffset;
  private final String offsetDurationExpression;
  private final String periodExpression;
  private Period window; // take into account time changes such as those due to daylight savings.
  private Duration offset; // do not adjust for daylight savings (Duration vs Period).
  private DateTime _now = null;

  void setClock(DateTime now) {
    this._now = now;
  }

  private DateTime now() {
    if (this._now == null) {
      return DateTime.now();
    } else {
      return this._now;
    }
  }

  public String getDataType() {
    return "datetime";
  }

  /**
   * At scan time, the interval (described by periodText) is used to effectively produce a Range Category as follows:
   * start-time = scan-time - interval
   * end-time = scan-time.
   *
   * @param periodText A standard ISO period specification, as defined in ISO 8601.
   *                   Examples: 3 months = P3M, 1 week = P1W, 1 day = P1D, 12 hours = PT12H.
   *
   * @see org.joda.time.Period (http://joda-time.sourceforge.net/api-release/index.html?org/joda/time/Period.html)
   */
  public RollingWindowCategoryDescriptor(String periodText) {
    this(periodText, "", false);
  }

  /**
   * The first parameter describes a time interval, I.  At scan time, this interval is used to effectively
   * produce a Range Category as follows:.
   * start-time = start-of-day(scan-time - I) + offsetDuration
   * end-time = start-of-day(scan-time) + offsetDuration
   *
   * @param periodExpression An extended ISO period specification, as defined in ISO 8601.
   * @param offsetDurationExpression A Duration representing the offset from the beginning of the day, as described above.
   */
  public RollingWindowCategoryDescriptor(String periodExpression, String offsetDurationExpression) {
    this(periodExpression, offsetDurationExpression, true);
  }

  private RollingWindowCategoryDescriptor(String periodExpression, String offsetDurationExpression, boolean isOffset) {
    this.periodExpression = periodExpression;
    this.offsetDurationExpression = offsetDurationExpression;
    this.isOffset = isOffset;
  }

  // We want to defer initialization until validation so that it will report faulty period expressions.
  private void ensureInitialized() {
    if (window == null) initialize();
  }

  private void initialize() {
    window = periodFormatter.parsePeriod(periodExpression);
    if (isOffset) {
      offset = periodFormatter.parsePeriod(offsetDurationExpression).toStandardDuration();
    }
  }

  @Override
  public void validate(String path) {
    try {
      initialize();
    } catch (Exception ex) {
      throw new ConfigValidationException(path,
          String.format("Invalid period or offset specified for Rolling Window: %s", ex.getLocalizedMessage()));
    }
  }

  @Override
  public boolean isSameType(CategoryDescriptor other) {
    return other instanceof RollingWindowCategoryDescriptor;
  }

  @Override
  public boolean isRefinement(CategoryDescriptor other) {
    if (isSameType(other)) {
      ensureInitialized();
      RollingWindowCategoryDescriptor otherDesc = (RollingWindowCategoryDescriptor) other;
      DateTime now = now();
      Duration myDuration = window.toDurationTo(now);
      Duration otherDuration = otherDesc.window.toDurationTo(now);
      if (myDuration.isLongerThan(otherDuration)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public CategoryDescriptor applyRefinement(CategoryDescriptor refinement) {
    if (!isRefinement(refinement)) throw new IllegalArgumentException(refinement + " is not a refinement of " + this);

    return refinement;
  }

  @Override
  public void validateConstraint(ScanConstraint constraint) {
    if (constraint instanceof DateRangeConstraint) {
      DateRangeConstraint requestedConstraint = (DateRangeConstraint) constraint;
      validateRangeContainment(requestedConstraint,
          requestedConstraint.getStart().toDateTimeAtStartOfDay(),
          requestedConstraint.getEnd().toDateTimeAtStartOfDay());
    } else if (constraint instanceof TimeRangeConstraint) {
      TimeRangeConstraint requestedConstraint = (TimeRangeConstraint) constraint;
      validateRangeContainment(requestedConstraint,
          requestedConstraint.getStart(),
          requestedConstraint.getEnd());
    } else {
      throw new InvalidConstraintException(constraint.getAttributeName(),
          "Rolling Window Categories only support Date and Time Range Constraints - provided constraint was " +
              constraint.getClass().getName());
    }
  }

  private void validateRangeContainment(RangeConstraint requested, DateTime start, DateTime end) {
    TimeRangeConstraint categoryAsConstraint = toConstraint(requested.getAttributeName());
    if (!categoryAsConstraint.containsRange(start, end)) {
      throw new InvalidConstraintException(requested.getAttributeName(),
          requested + " is not contained within " + categoryAsConstraint);
    }
  }

  private TimeRangeConstraint toConstraint(String name) {
    ensureInitialized();
    DateTime end = now();
    DateTime start = end.minus(window.toDurationTo(end));

    if (isOffset) {
      end = offsetFromMidnight(end, offset);
      start = offsetFromMidnight(start, offset);
    }

    return new TimeRangeConstraint(name, dateTimeFormatter.print(start), dateTimeFormatter.print(end));
  }

  private DateTime offsetFromMidnight(DateTime time, Duration offsetFromMidnight) {
    return time.toDateMidnight().toInterval().getStart().plus(offsetFromMidnight);
  }
}
