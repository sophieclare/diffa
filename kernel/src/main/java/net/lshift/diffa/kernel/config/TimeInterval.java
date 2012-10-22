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

package net.lshift.diffa.kernel.config;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 */
public class TimeInterval {
  private static final DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTimeNoMillis();
  private static final DateTimeFormatter dateFormatter = ISODateTimeFormat.date();

  public String start;
  public String end;

  public static TimeInterval fromJodaInterval(Interval interval) {
    String start = interval.getStart().toString(dateTimeFormatter);
    String end = interval.getEnd().toString(dateTimeFormatter);
    return new TimeInterval(start, end);
  }

  public TimeInterval(String start, String end) {
    this.start = start;
    this.end = end;
  }

  public TimeInterval overlap(Interval other) {
    Interval overlap = intervalFromRange(start, end).overlap(other);
    return TimeInterval.fromJodaInterval(overlap);
  }

  public boolean overlaps(Interval other) {
    return intervalFromRange(start, end).overlaps(other);
  }

  private Interval intervalFromRange(String start, String end) {
    String iStart = start != null ? start : "1900-01-01T00:00:00Z";
    String iEnd = end != null ? end : "3000-01-01T00:00:00Z";

    return new Interval(parseDateTime(iStart), parseDateTime(iEnd));
  }

  private DateTime parseDateTime(String expression) {
    try {
      return dateTimeFormatter.parseDateTime(expression);
    } catch (IllegalArgumentException ex) {
      return dateFormatter.parseDateTime(expression);
    }
  }
}
