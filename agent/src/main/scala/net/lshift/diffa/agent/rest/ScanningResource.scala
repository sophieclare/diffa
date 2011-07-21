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

package net.lshift.diffa.agent.rest

import org.springframework.beans.factory.annotation.Autowired
import net.lshift.diffa.kernel.actors.PairPolicyClient
import org.springframework.stereotype.Component
import net.lshift.diffa.docgen.annotations.{MandatoryParams, Description}
import net.lshift.diffa.docgen.annotations.MandatoryParams.MandatoryParam
import javax.ws.rs.{POST, PathParam, Path, DELETE}
import javax.ws.rs.core.Response
import net.lshift.diffa.kernel.config.ConfigStore
import org.slf4j.{LoggerFactory, Logger}

@Path("/scanning")
@Component
class ScanningResource {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  @Autowired var pairPolicyClient:PairPolicyClient = null
  @Autowired var configStore:ConfigStore = null

  @POST
  @Path("/pairs/{pairKey}/scan")
  @Description("Starts a scan for the given pair.")
  @MandatoryParams(Array(new MandatoryParam(name="pairKey", datatype="string", description="Pair Key")))
  def startScan(@PathParam("pairKey") pairKey:String) = {
    pairPolicyClient.scanPair(pairKey)

    Response.status(Response.Status.ACCEPTED).build
  }

  @POST
  @Path("/scan_all")
  @Description("Forces Diffa to execute a scan operation for every configured pair.")
  def scanAllPairings = {
    log.info("Initiating scan of all known pairs")
    configStore.listPairs.foreach(p => pairPolicyClient.scanPair(p.key))

    Response.status(Response.Status.ACCEPTED).build
  }

  @DELETE
  @Path("/pairs/{pairKey}/scan")
  @Description("Cancels any current and/or pending scans for the given pair.")
  @MandatoryParams(Array(new MandatoryParam(name="pairKey", datatype="string", description="Pair Key")))
  def cancelScanning(@PathParam("pairKey") pairKey:String) = {
    pairPolicyClient.cancelScans(pairKey)
    Response.status(Response.Status.OK).build
  }

}