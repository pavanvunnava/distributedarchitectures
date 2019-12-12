package org.dist.simplekafkapractice

import java.util

import org.I0Itec.zkclient.IZkChildListener
import scala.jdk.CollectionConverters._

case class BrokerChangeHandler(controller: KafkaController, zookeeperClient: ZookeeperClientImpl) extends IZkChildListener() {
  override def handleChildChange(parentPath: String, brokerList: util.List[String]): Unit = {
    val brokerIds = brokerList.asScala.map(_.toInt).toSet;
    //Some Optimization can be don here by just getting the broker information of broker ids that are not there in live brokers
    val brokers = brokerIds.map(zookeeperClient.getBrokerInfo(_))
    controller.onBrokerListUpdate(brokers);
  }
}
