package org.dist.simplekafkapractice
import com.fasterxml.jackson.core.`type`.TypeReference
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.dist.kvstore.JsonSerDes
import org.dist.queue.server.Config
import org.dist.queue.utils.ZKStringSerializer
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{Controller, ControllerExistsException, PartitionReplicas, ZookeeperClient}

import scala.jdk.CollectionConverters._

class ZookeeperClientImpl(config: Config) extends  ZookeeperClient {
  val BrokerTopicsPath = "/brokers/topics"
  val BrokerIdsPath = "/brokers/ids"
  val ControllerPath = "/controller"

  private val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer);


  def register(broker: Broker): Unit = {
    val brokerData = JsonSerDes.serialize(broker)
    val brokerPath = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }

  private def getBrokerPath(id:Int) = {
    BrokerIdsPath + "/" + id
  }

  def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createEphemeral(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
    }
  }

  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  override def tryCreatingControllerPath(controllerId: String)= {
    try {
      createEphemeralPath(zkClient, ControllerPath, controllerId)
    } catch {
      case e:ZkNodeExistsException => {
        val existingControllerId:String = zkClient.readData(ControllerPath)
        throw new ControllerExistsException(existingControllerId)
      }
    }
  }

  override def getAllBrokers(): Set[Broker] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(brokerId => {
      val data:String = zkClient.readData(getBrokerPath(brokerId.toInt))
      JsonSerDes.deserialize(data.getBytes, classOf[Broker])
    }).toSet
  }

  override def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }

  override def getBrokerInfo(brokerId: Int): Broker = {
    val data:String = zkClient.readData(getBrokerPath(brokerId))
    JsonSerDes.deserialize(data.getBytes, classOf[Broker])
  }

  private def getTopicPath(topicName: String) = {
    BrokerTopicsPath + "/" + topicName
  }

  override def getPartitionAssignmentsFor(topicName: String): List[PartitionReplicas] = {
    val partitionAssignments:String = zkClient.readData(getTopicPath(topicName))
    JsonSerDes.deserialize[List[PartitionReplicas]](partitionAssignments.getBytes, new TypeReference[List[PartitionReplicas]](){})
  }

  override def getAllBrokerIds(): Set[Int] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(_.toInt).toSet
  }

  override def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas]): Unit = {
    val topicsPath = getTopicPath(topicName)
    val topicsData = JsonSerDes.serialize(partitionReplicas)
    createPersistentPath(zkClient, topicsPath, topicsData)
  }

  def createPersistentPath(client: ZkClient, path: String, data: String = ""): Unit = {
    try {
      client.createPersistent(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createPersistent(path, data)
      }
    }
  }

  override def shutdown(): Unit = ???





  override def subscribeTopicChangeListener(listener: IZkChildListener): Option[List[String]] = ???

  override def subscribeControllerChangeListner(controller: Controller): Unit = ???

  override def registerSelf(): Unit = ???


}
