package org.dist.simplekafkapractice

import java.util

import org.I0Itec.zkclient.IZkChildListener
import scala.jdk.CollectionConverters._

case class BrokerChangeHandler(controller: KafkaController, zookeeperClient: ZookeeperClientImpl) extends IZkChildListener() {
  override def handleChildChange(parentPath: String, brokerList: util.List[String]): Unit = {
      val curBrokerIds = brokerList.asScala.map(_.toInt).toSet
      val newBrokerIds = curBrokerIds -- controller.liveBrokers.map(broker  => broker.id)
      val newBrokers = newBrokerIds.map(zookeeperClient.getBrokerInfo(_))
      newBrokers.foreach(controller.addBroker(_))
  }
}
