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

package net.lshift.diffa.agent.itest

import org.junit.Assume.assumeTrue
import support.TestEnvironments
import net.lshift.diffa.messaging.amqp.AmqpConnectionChecker
import org.junit.Test

/**
 * Test cases where various differences between a pair of participants are caused, and the agent is invoked
 * to detect and report on them. The participants in this test use the same versioning scheme, and thus will produce
 * the same versions for a given content item.
 */
class SameEnvironmentAmqpTest
    extends AbstractEnvironmentTest
    with CommonDifferenceTests {

  assumeTrue(AmqpConnectionChecker.isConnectionAvailable)

  def envFactory = TestEnvironments.same _
}