package org.dist.simplekafkapractice

import org.dist.queue.ZookeeperTestHarness
import org.dist.simplekafka.PartitionReplicas

class PartitionReplicationStrategyTest extends ZookeeperTestHarness{
test("Should evenly distribute the partitions and replicas across brokers") {

  val partitionSet: Set[PartitionReplicas] = Set(new PartitionReplicas(0, List(1,2,3)),
    new PartitionReplicas(1, List(2,3,1)),
    new PartitionReplicas(2, List(3,1,2)) );

  val replicaAssignmentStrategy:ReplicaAssignmentStrategy = new ReplicaAssignmentStrategy();
  val partitionReplicas = replicaAssignmentStrategy.assignReplicasForPartitions(3,  List[Int](1,2,3), 3)
  assert(partitionSet.equals(partitionReplicas));
}
}
