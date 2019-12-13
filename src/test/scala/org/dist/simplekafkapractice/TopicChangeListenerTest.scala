package org.dist.simplekafkapractice

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.simplekafka.PartitionReplicas
import org.dist.util.Networks
import org.mockito.{ArgumentMatchers, Mockito}

class TopicChangeListenerTest extends ZookeeperTestHarness {
  test("Controller should listen to topic changes") {
    var config: Config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config);

    val controller: KafkaController = new KafkaController(config, zookeeperClient);
    controller.Start();

    val broker1: KafkaBroker = new KafkaBroker(zookeeperClient, config);
    broker1.register();

    val replicationAssignmentStrategy: ReplicaAssignmentStrategy = Mockito.mock(classOf[ReplicaAssignmentStrategy])
    val replicaList: List[Int] = List(1, 2, 3);
    val partitionSet: Set[PartitionReplicas] = Set(new PartitionReplicas(1, replicaList))

    Mockito.when(replicationAssignmentStrategy.assignReplicasForPartitions(ArgumentMatchers.anyInt(),
      ArgumentMatchers.isNotNull,
      ArgumentMatchers.anyInt() ))
      .thenReturn(partitionSet);

    val topicManager: TopicManager = new TopicManager(zookeeperClient, replicationAssignmentStrategy)
    topicManager.CreateTopic("Order", 1, 3)

    TestUtils.waitUntilTrue(() => {
      controller.topics.size == 1
    }, "Waiting for all brokers to get added", 1000)

    assert(controller.topics.find(_ === "Order") != None);
  }
}
