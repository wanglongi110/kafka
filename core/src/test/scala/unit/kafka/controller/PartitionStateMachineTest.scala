package unit.kafka.controller

import java.util.Properties

import kafka.api.LeaderAndIsr
import kafka.common.TopicAndPartition
import kafka.controller._
import kafka.server.KafkaConfig
import kafka.utils.{MockTime, ReplicationUtils, TestUtils, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.metrics.Metrics
import org.junit.Assert._
import org.junit.{Before, Test}

class PartitionStateMachineTest extends ZooKeeperTestHarness {
  @Test
  def testUpdatingOfflinePartitionsCount() = {
    val props = KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect))
    var controller = new KafkaController(props, zkUtils, new MockTime(), new Metrics())
    var partitionStateMachine = new PartitionStateMachine(controller)

    val partitionIds = Set(0, 1, 2, 3)
    val topic = "test"
    val partitions = partitionIds.map(new TopicAndPartition("test", _))

    partitionStateMachine.handleStateChanges(partitions, NewPartition)
    partitionStateMachine.handleStateChanges(partitions, OfflinePartition)
    assertEquals(s"There should be ${partitions.size} offline partition(s)", partitions.size, partitionStateMachine.offlinePartitionCount)

    /**
      * populate the leadership info for the partitions on ZK so that election of new leader can succeed
      */
    partitions.foreach { partition =>
      zkUtils.createPersistentPath(ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition.partition))
      val (updateSucceed, newZkVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partition.partition,
        new LeaderAndIsr(0, 0, List(0, 1, 2), 0), controller.epoch, 0)
    }
    partitionStateMachine.handleStateChanges(partitions, OnlinePartition)
    assertEquals(s"There should be no offline partition(s)", 0, partitionStateMachine.offlinePartitionCount)
  }

  @Test
  def testNoOfflinePartitionsChangeForTopicsBeingDeleted() = {
    val brokerProps = TestUtils.createBrokerConfig(0, zkConnect)
    brokerProps.setProperty("delete.topic.enable", "true")
    val props = KafkaConfig.fromProps(brokerProps)

    var controller = new KafkaController(props, zkUtils, new MockTime(), new Metrics())
    var partitionStateMachine = new PartitionStateMachine(controller)

    val partitionIds = Set(0, 1, 2, 3)
    val topic = "test"
    val partitions = partitionIds.map(new TopicAndPartition("test", _))

    controller.topicDeletionManager.topicsToBeDeleted.put(topic, 0L)

    partitionStateMachine.handleStateChanges(partitions, NewPartition)
    partitionStateMachine.handleStateChanges(partitions, OfflinePartition)
    assertEquals(s"There should be no offline partition(s)", 0, partitionStateMachine.offlinePartitionCount)
  }


}
