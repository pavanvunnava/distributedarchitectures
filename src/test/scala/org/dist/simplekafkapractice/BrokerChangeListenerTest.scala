package org.dist.simplekafkapractice

import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks

class BrokerChangeListenerTest extends ZookeeperTestHarness {
  test("Controller should track changes in Broker registrations") {
    var config: Config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config);

    val controller: KafkaController = new KafkaController(config, zookeeperClient,null);
    controller.Start();

    val broker: KafkaBroker = new KafkaBroker(zookeeperClient, config);
    broker.register();

    TestUtils.waitUntilTrue(() => {
      controller.liveBrokers.size == 1
    }, "Waiting for all brokers to get added", 1000)

    assert(controller.liveBrokers.size == 1)

    var config2: Config = new Config(2, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
    val zookeeperClient2: ZookeeperClientImpl = new ZookeeperClientImpl(config2);

    val broker2: KafkaBroker = new KafkaBroker(zookeeperClient2, config2);
    broker2.register();

    TestUtils.waitUntilTrue(() => {
      controller.liveBrokers.size == 2
    }, "Waiting for all brokers to get added", 1000)

    assert(controller.liveBrokers.size == 2)
  }
}
