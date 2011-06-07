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

package net.lshift.diffa.kernel.frontend.wire

import net.lshift.diffa.kernel.participants.ParticipantFactory
import net.lshift.diffa.kernel.client.{Actionable, ActionableRequest, ActionsClient}
import net.lshift.diffa.kernel.config.{RepairAction, Pair, ConfigStore}
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import javax.management.remote.rmi._RMIConnection_Stub

/**
 * This is a conduit to the actions that are provided by participants
 */
class ActionsProxy(val config:ConfigStore, val factory:ParticipantFactory) extends ActionsClient {

  def listActions(pairKey: String): Seq[Actionable] =
    withValidPair(pairKey) { pair =>
      config.listRepairActionsForPair(pair).map(Actionable.fromRepairAction)
    }

  def listEntityScopedActions(pairKey: String) = listActions(pairKey).filter(_.scope == RepairAction.ENTITY_SCOPE)

  def listPairScopedActions(pairKey: String) = listActions(pairKey).filter(_.scope == RepairAction.PAIR_SCOPE)

  def invoke(request:ActionableRequest) : InvocationResult =
    withValidPair(request.pairKey) { pair =>
      val participant = factory.createUpstreamParticipant(pair.upstream)
      try {
        participant.invoke(request.actionId, request.entityId)
      }
      finally {
        participant.close()
      }
    }

  def withValidPair[T](pairKey: String)(f: Pair => T): T = {
    val pair = config.getPair(pairKey)
    f(pair)
  }

}
