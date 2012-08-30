package net.lshift.diffa.agent.auth

/**
 * Definitions of the various supported Diffa privileges.
 */
trait Privilege {
  /** The name of the privilege **/
  def name:String
}

/**
 * A privilege that is applied against a space.
 */
case class SpacePrivilege(name:String) extends Privilege

/**
 * A privilege that is applied against an action.
 */
case class ActionPrivilege(name:String) extends Privilege

/**
 * A privilege that is applied against a pair.
 */
case class PairPrivilege(name:String) extends Privilege

/**
 * A privilege that is applied against an endpoint.
 */
case class EndpointPrivilege(name:String) extends Privilege

/** Catalogue of privileges supported by Diffa */
object Privileges {
  val READ_DIFFS = PairPrivilege("read-diffs")  // Ability to query for differences, access overviews, etc
  val CONFIGURE = SpacePrivilege("configure")   // Ability to configure a space
  val INITIATE_SCAN = PairPrivilege("initiate-scan")
  val POST_CHANGE_EVENT = EndpointPrivilege("post-change-event")
  val POST_INVENTORY = EndpointPrivilege("post-inventory")
  val SCAN_STATUS = PairPrivilege("view-scan-status")
  val DIAGNOSTICS = PairPrivilege("view-diagnostics")
  val INVOKE_ACTIONS = ActionPrivilege("invoke-actions")
  val IGNORE_DIFFS = PairPrivilege("ignore-diffs")
  val VIEW_EXPLANATIONS = PairPrivilege("view-explanations")
}