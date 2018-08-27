/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    controller.topicDeletionManager.topicsToBeDeleted += topic

    partitionStateMachine.handleStateChanges(partitions, NewPartition)
    partitionStateMachine.handleStateChanges(partitions, OfflinePartition)
    assertEquals(s"There should be no offline partition(s)", 0, partitionStateMachine.offlinePartitionCount)
  }


}
