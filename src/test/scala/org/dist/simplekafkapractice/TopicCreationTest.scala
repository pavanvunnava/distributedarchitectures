package org.dist.simplekafkapractice

import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.PartitionReplicas
import org.dist.util.Networks
import org.mockito.{ArgumentMatchers, Mockito}

import scala.jdk.CollectionConverters._

class TopicCreationTest extends ZookeeperTestHarness {
test("should create a topic in a zookeeper") {
  var config: Config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
  val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config);

  val controller1:KafkaController  = new KafkaController(config, zookeeperClient);
  controller1.Start();

  val broker1: KafkaBroker = new KafkaBroker(zookeeperClient, config);
  broker1.register();

  TestUtils.waitUntilTrue(() => {
    controller1.liveBrokers.size ==1
  }, "Waiting for all brokers to get added", 1000)


  val replicationAssignmentStrategy: ReplicaAssignmentStrategy = Mockito.mock(classOf[ReplicaAssignmentStrategy])
  val replicaList: List[Int] = List(1,2,3);
  val partitionSet: Set[PartitionReplicas] = Set(new PartitionReplicas(1, replicaList));

  Mockito.when(replicationAssignmentStrategy.assignReplicasForPartitions(ArgumentMatchers.anyInt(),
    ArgumentMatchers.isNotNull,
    ArgumentMatchers.anyInt() ))
    .thenReturn(partitionSet);

  val topicManager:TopicManager = new TopicManager(zookeeperClient, replicationAssignmentStrategy);
  topicManager.CreateTopic("Order", 4, 3);

  assert(zookeeperClient.getPartitionAssignmentsFor("Order").toSet.equals(partitionSet));

}
}
