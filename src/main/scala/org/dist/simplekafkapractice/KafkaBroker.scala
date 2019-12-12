package org.dist.simplekafkapractice

import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker

case class KafkaBroker(zkClient: ZookeeperClientImpl, config:Config) {
  val zookeeperClient: ZookeeperClientImpl = zkClient;

  def register(): Unit = {
    var broker:Broker = new Broker(config.brokerId,config.hostName,config.port);
    zkClient.register(broker);
  }

}
