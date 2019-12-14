package org.dist.simplekafkapractice

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.simplekafka.Partition
import org.dist.util.Networks

class PartitionReadWriteTest extends ZookeeperTestHarness {
  test("should read and write messages to partition"){
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), "", List(TestUtils.tempDir().getAbsolutePath))

    val partition = new Partition(config1, TopicAndPartition("topic1", 0))
    val offset = partition.append("key1", "message1")
    val offset1 = partition.append("key2", "message2")
    val messages: Seq[partition.Row] = partition.read()
    assert(messages.size == 2)
    assert(messages(0).key == "key1")
    assert(messages(1).key == "key2")
    assert(messages(0).value == "message1")
    assert(messages(1).value == "message2")
  }

  test("should read a partition from an offset"){
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), "", List(TestUtils.tempDir().getAbsolutePath))

    val partition = new Partition(config1, TopicAndPartition("topic1", 0))
    val offset = partition.append("key1", "message1")
    val offset1 = partition.append("key2", "message2")
    val messages: Seq[partition.Row] = partition.read(offset1)
    assert(messages.size == 1)
    assert(messages(0).key == "key2")
    assert(messages(0).value == "message2")
  }
}
