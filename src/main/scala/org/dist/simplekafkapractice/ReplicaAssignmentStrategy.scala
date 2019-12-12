package org.dist.simplekafkapractice

import org.dist.simplekafka.PartitionReplicas

class ReplicaAssignmentStrategy {

  def assignReplicasForPartitions(partitions: Int, brokerList: Set[Int], replicationFactor:Int): Set[PartitionReplicas] = {
    Set()
  }
}
