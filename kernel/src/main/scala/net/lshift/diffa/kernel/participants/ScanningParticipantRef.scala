package net.lshift.diffa.kernel.participants

import net.lshift.diffa.adapter.scanning.{ScanConstraint, ScanResultEntry}

/**
 * Provides a reference to a scanning adapter. An implementation of this will be provided via a
 * ScanningParticipantFactory implementation, and will generally be an accessor to a remote resource. The
 * implementation of this will be responsible for handling argument serialization, RPC execution and result
 * deserialization.
 */
trait ScanningParticipantRef {
  /**
   * Scans this adapter with the given constraints and aggregations.
   */
  def scan(constraints:Seq[ScanConstraint], aggregations:Seq[CategoryFunction]): Seq[ScanResultEntry]
}

/**
 * Factory for creating scanning adapter references.
 */
trait ScanningParticipantFactory extends AddressDrivenFactory[ScanningParticipantRef]