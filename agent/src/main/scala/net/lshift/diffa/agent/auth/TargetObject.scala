package net.lshift.diffa.agent.auth

/**
 * Describes an object that can be the target of a privilege check.
 */
trait TargetObject
class SpaceTarget(val space:Long) extends TargetObject
class PairTarget(space:Long, val pair:String) extends SpaceTarget(space)
class EndpointTarget(space:Long, val endpoint:String) extends SpaceTarget(space)
class ActionTarget(space:Long, pair:String, val action:String) extends PairTarget(space, pair)
class ReportTarget(space:Long, pair:String, val report:String) extends PairTarget(space, pair)

/**
 * TODO: This should actually include a reference to the owning pair, but we don't tend to have that in API calls. So
 * this target provides a typed reference to all locations that need to be corrected.
 */
class DiffTarget(space:Long, val evtSeqId:String) extends SpaceTarget(space)

class UserTarget(val username:String) extends TargetObject