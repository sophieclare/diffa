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

import org.springframework.stereotype.Component
import org.springframework.beans.factory.annotation.Autowired
import javax.ws.rs.core.{Response, UriInfo, Context}
import javax.ws.rs._
import net.lshift.diffa.kernel.client.ActionsClient
import net.lshift.diffa.kernel.differencing.DifferencesManager
import net.lshift.diffa.kernel.diag.DiagnosticsManager
import net.lshift.diffa.kernel.actors.PairPolicyClient
import net.lshift.diffa.kernel.frontend.{EscalationDef, Changes, Configuration}
import org.springframework.security.access.prepost.PreAuthorize
import net.lshift.diffa.kernel.reporting.ReportManager
import com.sun.jersey.api.NotFoundException
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.UserDetails
import org.slf4j.LoggerFactory
import net.lshift.diffa.kernel.util.AlertCodes._
import net.lshift.diffa.kernel.config.{DomainCredentialsManager, User, DomainConfigStore}
import net.lshift.diffa.kernel.config.system.CachedSystemConfigStore
import net.lshift.diffa.kernel.limiting.DomainRateLimiterFactory
import net.lshift.diffa.kernel.config.User
import net.lshift.diffa.kernel.config.User
import net.lshift.diffa.agent.rest.ResponseUtils._
import net.lshift.diffa.kernel.config.User
import net.lshift.diffa.kernel.frontend.EscalationDef

/**
 * The policy is that we will publish spaces as the replacement term for domains
 * but to avoid having to refactor a bunch of of code straight away, we'll just change
 * the path specification from /domains to /spaces and implement a redirect.
 */

/**
 * NOTE TO MAINTENANCE ENGINEER:
 *
 * In the version of Jersey that this resource was coded against (1.13), you cannot seem to specify
 * Path("/spaces/{space:.+}") on the class resource - this just results in a non-match and the framework
 * returns a 405 to the client. However, if you specify the {space:.+} regex on a method level, Jersey matches
 * this just fine and in addition, the PreAuthorize binding, which is specified on the class level, seems to
 * work as well.
 *
 * For the sake of clarity, it might be an idea to see whether Jersey 2.0 can handle this better.
 */
@Path("/spaces/")
@Component
@PreAuthorize("hasPermission(#space, 'domain-user')")
class SpaceResource {

  val log = LoggerFactory.getLogger(getClass)

  @Context var uriInfo:UriInfo = null

  @Autowired var config:Configuration = null
  @Autowired var credentialsManager:DomainCredentialsManager = null
  @Autowired var actionsClient:ActionsClient = null
  @Autowired var differencesManager:DifferencesManager = null
  @Autowired var diagnosticsManager:DiagnosticsManager = null
  @Autowired var pairPolicyClient:PairPolicyClient = null
  @Autowired var domainConfigStore:DomainConfigStore = null
  @Autowired var systemConfigStore:CachedSystemConfigStore = null
  @Autowired var changes:Changes = null
  @Autowired var changeEventRateLimiterFactory: DomainRateLimiterFactory = null
  @Autowired var reports:ReportManager = null

  private def getCurrentUser(space:String) : String = SecurityContextHolder.getContext.getAuthentication.getPrincipal match {
    case user:UserDetails => user.getUsername
    case token:String     => {
      systemConfigStore.getUserByToken(token) match {
        case user:User => user.getName
        case _         =>
          log.warn(formatAlertCode(space, SPURIOUS_AUTH_TOKEN) + " " + token)
          null
      }
    }
    case _                => null
  }

  private def withValidSpace[T](space: String, resource: T) =
    if (config.doesDomainExist(space))
      resource
    else
      throw new NotFoundException("Invalid space")

  private def withValidSpace[T](space: String, f: () => T) =
    if (config.doesDomainExist(space))
      f()
    else
      throw new NotFoundException("Invalid space")


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // The following routes are implemented within the context of this top level resource.
  //
  // These preceding resources would ideally be implemented in an appropriate sub-resource, but due to the eager
  // matching of {space:.+}, if you want to match a trailing pattern in a sub-resource that has the same name as the
  // as a top level API pattern, then you need to specify the very specific match in this class, in order to guarantee
  // match precedence.
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  @POST
  @Path("/{space:.+}/config/pairs/{id}/escalations")
  @Consumes(Array("application/json"))
  def createEscalation(@Context uri:UriInfo,
                       @PathParam("space") space:String,
                       @PathParam("id") id:String,
                       e: EscalationDef) = {
    withValidSpace(space, () => {
      config.createOrUpdateEscalation(space, id, e)
      resourceCreated(e.name, uri)
    })
  }

  @DELETE
  @Path("/{space:.+}/config/pairs/{pairKey}/escalations/{name}")
  def deleteEscalation(@PathParam("space") space:String,
                       @PathParam("name") name: String,
                       @PathParam("pairKey") pairKey: String) = {
    withValidSpace(space, () => {
      config.deleteEscalation(space, name, pairKey)
    })
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // The following routes are implemented by delegating to sub-resources.
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Path("/{space:.+}/config")
  def getConfigResource(@Context uri:UriInfo,
                        @PathParam("space") space:String) =
    withValidSpace(space, new ConfigurationResource(config, space, getCurrentUser(space), uri))

  @Path("/{space:.+}/credentials")
  def getCredentialsResource(@Context uri:UriInfo,
                             @PathParam("space") space:String) =
    withValidSpace(space, new CredentialsResource(credentialsManager, space, uri))

  @Path("/{space:.+}/diffs")
  def getDifferencesResource(@Context uri:UriInfo,
                             @PathParam("space") space:String) =
    withValidSpace(space, new DifferencesResource(differencesManager, domainConfigStore, space, uri))

  @Path("/{space:.+}/escalations")
  def getEscalationsResource(@PathParam("space") space:String) =
    withValidSpace(space, new EscalationsResource(config, space))

  @Path("/{space:.+}/actions")
  def getActionsResource(@Context uri:UriInfo,
                         @PathParam("space") space:String) =
    withValidSpace(space, new ActionsResource(actionsClient, space, uri))

  @Path("/{space:.+}/reports")
  def getReportsResource(@Context uri:UriInfo,
                         @PathParam("space") space:String) =
    withValidSpace(space, new ReportsResource(domainConfigStore, reports, space, uri))

  @Path("/{space:.+}/diagnostics")
  def getDiagnosticsResource(@PathParam("space") space:String) =
    withValidSpace(space, new DiagnosticsResource(diagnosticsManager, config, space))

  @Path("/{space:.+}/scanning")
  def getScanningResource(@PathParam("space") space:String) =
    withValidSpace(space, new ScanningResource(pairPolicyClient, config, domainConfigStore, diagnosticsManager, space, getCurrentUser(space)))

  @Path("/{space:.+}/changes")
  def getChangesResource(@PathParam("space") space:String) = {
    withValidSpace(space, new ChangesResource(changes, space, changeEventRateLimiterFactory))
  }

  @Path("/{space:.+}/inventory")
  def getInventoryResource(@PathParam("space") space:String) =
    withValidSpace(space, new InventoryResource(changes, domainConfigStore, space))

  @Path("/{space:.+}/limits")
  def getLimitsResource(@PathParam("space") space:String) =
    withValidSpace(space, new DomainServiceLimitsResource(config, space))
}