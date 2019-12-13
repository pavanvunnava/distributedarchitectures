package org.dist.simplekafkapractice

import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{Controller, ControllerExistsException, PartitionReplicas, TopicChangeHandler, ZookeeperClient}

class KafkaController(config: Config, zookeeperClient: ZookeeperClientImpl) {

  var currentLeader = -1
  var topics:scala.collection.immutable.Set[String] = scala.collection.immutable.Set();
  var liveBrokers:scala.collection.mutable.Set[Broker] = scala.collection.mutable.Set()
  def Start() = {
    electMe();
  }

  private def electMe() = {

    val brokerId = config.brokerId;
    val leaderId = s"$brokerId";

    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      onBecomingLeader()
    } catch {
      case e: ControllerExistsException => {
        this.currentLeader = e.controllerId.toInt
      }
    }
  }

  private def onBecomingLeader() = {
    this.currentLeader = config.brokerId;
    zookeeperClient.subscribeBrokerChangeListener(new BrokerChangeHandler(this, zookeeperClient))
    zookeeperClient.subscribeTopicChangeListener(new TopicChangeHandler(zookeeperClient, onTopicChange))
  }

  def onTopicChange(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
     topics = topics + topicName;
  }
   def onBrokerListUpdate(brokerList: Set[Broker]): Unit = {
    liveBrokers.clear()
    liveBrokers = liveBrokers ++ brokerList;
  }
}

