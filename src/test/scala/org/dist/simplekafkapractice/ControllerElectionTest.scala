package org.dist.simplekafkapractice

import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks
import org.scalatest.Matchers

class ControllerElectionTest extends ZookeeperTestHarness{

  test("Should elect the controller as leader") {

    var config:Config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config);

    val controller1:KafkaController  = new KafkaController(config, zookeeperClient, null);
    controller1.Start();

    val expectedLeader =1;

    assert(controller1.currentLeader == expectedLeader);

  }

  test("Should elect the first controller that creates the controller path, as leader") {
    var config:Config = new Config(13, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config);

    val controller1:KafkaController  = new KafkaController(config, zookeeperClient, null);
    controller1.Start();

    var config2:Config = new Config(9, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
    val controller2:KafkaController  = new KafkaController(config2, zookeeperClient, null);
    controller2.Start();

    val expectedLeader =13;

    assert(controller1.currentLeader == expectedLeader);
  }

  test("Should elect the first controller that creates the controller path, as leader. Second controller should know first controller is the leader") {
    var config:Config = new Config(13, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config);

    val controller1:KafkaController  = new KafkaController(config, zookeeperClient, null);
    controller1.Start();


    var config2:Config = new Config(9, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath));
    val zookeeperClient2: ZookeeperClientImpl = new ZookeeperClientImpl(config2);
    val controller2:KafkaController  = new KafkaController(config2, zookeeperClient2, null);
    controller2.Start();

    val expectedLeader =13;

    assert(controller2.currentLeader == expectedLeader);
  }
}
