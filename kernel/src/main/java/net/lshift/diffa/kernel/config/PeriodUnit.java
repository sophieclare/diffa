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

import java.util.ArrayList;

import org.joda.time.DateTime;
import org.joda.time.Interval;

public abstract class PeriodUnit {
	public static PeriodUnit INDIVIDUAL = new IndividualPeriodUnit();
	public static PeriodUnit DAILY = new DailyPeriodUnit();
	public static PeriodUnit MONTHLY = new MonthlyPeriodUnit();
	public static PeriodUnit YEARLY = new YearlyPeriodUnit();
	private static ArrayList<PeriodUnit> periods = new ArrayList<PeriodUnit>();

	static {
		periods.add(YEARLY);
		periods.add(MONTHLY);
		periods.add(DAILY);
		periods.add(INDIVIDUAL);
	}

	private String name;

	PeriodUnit(String name) {
		this.name = name;
	}

	public static PeriodUnit maximumCoveredPeriod(Interval interval) {
		for (PeriodUnit u: periods) {
			if (u.isCovering(interval)) {
				return u;
			}
		}
		return PeriodUnit.INDIVIDUAL;
	}

	public boolean isCovering(Interval interval) {
		return interval != null && subtractOneUnit(floor(interval.getEnd())).compareTo(ceiling(interval.getStart())) >= 0;
	}

	protected abstract DateTime subtractOneUnit(DateTime from);

	protected abstract DateTime floor(DateTime instant);

	protected abstract DateTime ceiling(DateTime instant);

	@Override
	public String toString() {
		return name;
	}
}
