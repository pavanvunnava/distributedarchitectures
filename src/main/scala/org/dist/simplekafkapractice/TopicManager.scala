package org.dist.simplekafkapractice

case class TopicManager(zookeeperClient: ZookeeperClientImpl, replicaAssignmentStrategy: ReplicaAssignmentStrategy) {

  def CreateTopic(topicName: String, noOfPartitions: Int, replicationFactor:Int) ={
    val brokerIds = zookeeperClient.getAllBrokerIds();
    val partitionReplicas = replicaAssignmentStrategy.assignReplicasForPartitions(noOfPartitions,brokerIds, replicationFactor);
    // register topic with partition assignments to zookeeper
    zookeeperClient.setPartitionReplicasForTopic(topicName, partitionReplicas)
  }
}
