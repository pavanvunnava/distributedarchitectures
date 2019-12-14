package org.dist.simplekafkapractice

import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.annotations.VisibleForTesting
import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{Controller, ControllerExistsException, LeaderAndReplicaRequest, LeaderAndReplicas, PartitionInfo, PartitionReplicas, SimpleSocketServer, TopicChangeHandler, ZookeeperClient}

import scala.jdk.CollectionConverters._

class KafkaController(config: Config, zookeeperClient: ZookeeperClientImpl, socketServer: SimpleSocketServer) {
  val correlationId = new AtomicInteger(0)
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
    topics = topics+topicName;
    val leaderAndReplicas = selectLeaderAndFollowerBrokersForPartitions(topicName, partitionReplicas)
    sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas, partitionReplicas)
  }

  def sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas:Seq[LeaderAndReplicas], partitionReplicas: Seq[PartitionReplicas]) = {
    val brokerToLeaderIsrRequest = new util.HashMap[Broker, java.util.List[LeaderAndReplicas]]()
    leaderAndReplicas.foreach(lr ⇒ {
      lr.partitionStateInfo.allReplicas.foreach(broker ⇒ {
        var leaderReplicas = brokerToLeaderIsrRequest.get(broker)
        if (leaderReplicas == null) {
          leaderReplicas = new util.ArrayList[LeaderAndReplicas]()
          brokerToLeaderIsrRequest.put(broker, leaderReplicas)
        }
        leaderReplicas.add(lr)
      })
    })
    val brokers = brokerToLeaderIsrRequest.keySet().asScala
    for(broker ← brokers) {
      val leaderAndReplicas: java.util.List[LeaderAndReplicas] = brokerToLeaderIsrRequest.get(broker)
      val leaderAndReplicaRequest = LeaderAndReplicaRequest(leaderAndReplicas.asScala.toList)
      val request = RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicaRequest), correlationId.getAndIncrement())
      socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
    }
  }

  @VisibleForTesting
   def selectLeaderAndFollowerBrokersForPartitions(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    val leaderAndReplicas: Seq[LeaderAndReplicas] = partitionReplicas.map(p => {
      val leaderBrokerId = p.brokerIds.head //This is where leader for particular partition is selected
      val leaderBroker = getBroker(leaderBrokerId)
      val replicaBrokers = p.brokerIds.map(id ⇒ getBroker(id))
      LeaderAndReplicas(TopicAndPartition(topicName, p.partitionId), PartitionInfo(leaderBroker, replicaBrokers))
    })
    leaderAndReplicas
  }

  def addBroker(broker: Broker) = {
    liveBrokers += broker
  }
  private def getBroker(brokerId:Int) = {
    liveBrokers.find(b ⇒ b.id == brokerId).get
  }


}

