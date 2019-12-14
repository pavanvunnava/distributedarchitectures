package org.dist.simplekafkapractice

import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.{PartitionReplicas, TestSocketServer}
import org.dist.util.Networks
import org.mockito.{ArgumentMatchers, Mockito}

class SendLeadersAndReplicasTest  extends ZookeeperTestHarness{
test("To test the leader and replicas information is sent to appropriate brokers for a given partition"){
  var config: Config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
  val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config);

  val testSocketServer: TestSocketServer = new TestSocketServer(config)
  val controller: KafkaController = new KafkaController(config, zookeeperClient, testSocketServer);
  controller.Start();

  val broker1: KafkaBroker = new KafkaBroker(zookeeperClient, config);
  broker1.register();

  TestUtils.waitUntilTrue(() => {
    controller.liveBrokers.size == 1
  }, "Waiting for all brokers to get added", 5000)

  var config2: Config = new Config(2, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
  val zookeeperClient2: ZookeeperClientImpl = new ZookeeperClientImpl(config2);

  val broker2: KafkaBroker = new KafkaBroker(zookeeperClient2, config2);
  broker2.register();

  TestUtils.waitUntilTrue(() => {
    controller.liveBrokers.size == 2
  }, "Waiting for all brokers to get added", 5000)

  var config3: Config = new Config(3, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
  val zookeeperClient3: ZookeeperClientImpl = new ZookeeperClientImpl(config3);

  val broker3: KafkaBroker = new KafkaBroker(zookeeperClient3, config3);
  broker3.register();

  TestUtils.waitUntilTrue(() => {
    controller.liveBrokers.size == 3
  }, "Waiting for all brokers to get added", 5000)




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

  assert(testSocketServer.messages.size() === 3);
}
}
