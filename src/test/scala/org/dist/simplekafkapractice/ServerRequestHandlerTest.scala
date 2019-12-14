package org.dist.simplekafkapractice

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.simplekafka.{PartitionReplicas, ReplicaManager, SimpleKafkaApi, SimpleSocketServer, TestSocketServer}
import org.dist.util.Networks
import org.mockito.{ArgumentMatchers, Mockito}
import sun.java2d.pipe.SpanShapeRenderer.Simple

class ServerRequestHandlerTest  extends ZookeeperTestHarness{
  test("Controller should send Lead and Replicas Request to broker") {
    var config: Config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config);

    val replicaManager:ReplicaManager = new ReplicaManager(config)

    val socketServer: SimpleSocketServer = new SimpleSocketServer(config.brokerId,config.hostName,config.port, new SimpleKafkaApi(config, replicaManager))
    socketServer.startup()
    val controller1: KafkaController = new KafkaController(config, zookeeperClient, socketServer);
    controller1.Start();

    var config2: Config = new Config(2, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
    val zookeeperClient2: ZookeeperClientImpl = new ZookeeperClientImpl(config2);

    val replicaManager2:ReplicaManager = new ReplicaManager(config2)

    val socketServer2: SimpleSocketServer = new SimpleSocketServer(config.brokerId,config.hostName,config.port, new SimpleKafkaApi(config, replicaManager2))
    val controller2: KafkaController = new KafkaController(config2, zookeeperClient2, socketServer2);
    controller2.Start();
    socketServer2.startup()

    val broker1: KafkaBroker = new KafkaBroker(zookeeperClient, config);
    broker1.register();

    TestUtils.waitUntilTrue(() => {
      controller1.liveBrokers.size == 1
    }, "Waiting for all brokers to get added", 5000)


    val broker2: KafkaBroker = new KafkaBroker(zookeeperClient2, config2);
    broker2.register();



    val replicationAssignmentStrategy: ReplicaAssignmentStrategy = Mockito.mock(classOf[ReplicaAssignmentStrategy])
    val replicaList: List[Int] = List(1, 2);
    val partitionSet: Set[PartitionReplicas] = Set(new PartitionReplicas(1, replicaList))

    Mockito.when(replicationAssignmentStrategy.assignReplicasForPartitions(ArgumentMatchers.anyInt(),
      ArgumentMatchers.isNotNull,
      ArgumentMatchers.anyInt() ))
      .thenReturn(partitionSet);

    val topicManager: TopicManager = new TopicManager(zookeeperClient, replicationAssignmentStrategy)
    topicManager.CreateTopic("Order", 1, 2)

    TestUtils.waitUntilTrue(() => {
      controller1.topics.size == 1
    }, "Waiting for all brokers to get added", 1000)

    TestUtils.waitUntilTrue(() => {
      replicaManager.allPartitions.size() == 1
    }, "Waiting for all brokers to get added", 1000)

    assert(replicaManager.allPartitions.size() == 1);
    assert(replicaManager2.allPartitions.size() == 1);
  }

}
