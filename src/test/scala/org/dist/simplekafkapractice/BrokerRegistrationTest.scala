package org.dist.simplekafkapractice

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.util.Networks

class BrokerRegistrationTest extends ZookeeperTestHarness{
 test("should test if broker is registered with zookeeper") {

   val config:Config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
   val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config);

   val broker: KafkaBroker = new KafkaBroker(zookeeperClient, config);
   broker.register();

   val brokers:Set[Broker] = zookeeperClient.getAllBrokers();
   assert(brokers.size === 1 );
   assert(brokers.find(_.id === 1) != None);

 }
  test("should test if multiples brokers are registered with zookeeper") {

    val config:Config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config);

    val broker1: KafkaBroker = new KafkaBroker(zookeeperClient, config);
    broker1.register();

    val config2:Config = new Config(2, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
    val zookeeperClient2: ZookeeperClientImpl = new ZookeeperClientImpl(config);

    val broker2: KafkaBroker = new KafkaBroker(zookeeperClient2, config2);
    broker2.register();

    val brokers:Set[Broker] = zookeeperClient.getAllBrokers();
    assert(brokers.size === 2 );
    assert(brokers.find(_.id === 1) != None);
    assert(brokers.find(_.id === 2) != None);

    val brokers2:Set[Broker] = zookeeperClient2.getAllBrokers();
    assert(brokers2.size === 2 );
    assert(brokers2.find(_.id === 1) != None);
    assert(brokers2.find(_.id === 2) != None);

  }


}
