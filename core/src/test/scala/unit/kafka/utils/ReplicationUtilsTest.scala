/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import kafka.controller.{ControllerContext, LeaderIsrAndControllerEpoch}
import kafka.api.LeaderAndIsr
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.errors.ControllerMovedException
import org.junit.Assert._
import org.junit.{Before, Test}


class ReplicationUtilsTest extends ZooKeeperTestHarness {
  val topic = "my-topic-test"
  val partitionId = 0
  val brokerId = 1
  val leaderEpoch = 1
  val controllerEpoch = 1
  val zkVersion = 1
  val topicPath = "/brokers/topics/my-topic-test/partitions/0/state"
  val topicData = Json.encode(Map("controller_epoch" -> 1, "leader" -> 1,
    "versions" -> 1, "leader_epoch" -> 1,"isr" -> List(1,2)))
  val topicDataVersionMismatch = Json.encode(Map("controller_epoch" -> 1, "leader" -> 1,
    "versions" -> 2, "leader_epoch" -> 1,"isr" -> List(1,2)))
  val topicDataMismatch = Json.encode(Map("controller_epoch" -> 1, "leader" -> 1,
    "versions" -> 2, "leader_epoch" -> 2,"isr" -> List(1,2)))

  val topicDataLeaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(1,leaderEpoch,List(1,2),0), controllerEpoch)

  @Before
  override def setUp() {
    super.setUp()
    zkUtils.createPersistentPath(topicPath, topicData)
  }

  @Test
  def testUpdateLeaderAndIsr() {
    zkUtils.makeSurePersistentPathExists(ZkUtils.IsrChangeNotificationPath)

    val replicas = List(0,1)

    // regular update
    val newLeaderAndIsr1 = new LeaderAndIsr(brokerId, leaderEpoch, replicas, 0)
    val (updateSucceeded1,newZkVersion1) = ReplicationUtils.transactionalUpdateLeaderAndIsr(zkUtils,
      "my-topic-test", partitionId, newLeaderAndIsr1, 0, controllerEpoch)
    assertTrue(updateSucceeded1)
    assertEquals(newZkVersion1, 1)

    // mismatched zkVersion with the same data
    val newLeaderAndIsr2 = new LeaderAndIsr(brokerId, leaderEpoch, replicas, zkVersion + 1)
    val (updateSucceeded2,newZkVersion2) = ReplicationUtils.transactionalUpdateLeaderAndIsr(zkUtils,
      "my-topic-test", partitionId, newLeaderAndIsr2, zkVersion + 1, controllerEpoch)
    assertTrue(updateSucceeded2)
    // returns true with existing zkVersion
    assertEquals(newZkVersion2,1)

    // mismatched zkVersion and leaderEpoch
    val newLeaderAndIsr3 = new LeaderAndIsr(brokerId, leaderEpoch + 1, replicas, zkVersion + 1)
    val (updateSucceeded3,newZkVersion3) = ReplicationUtils.transactionalUpdateLeaderAndIsr(zkUtils,
      "my-topic-test", partitionId, newLeaderAndIsr3, zkVersion + 1, controllerEpoch)
    assertFalse(updateSucceeded3)
    assertEquals(newZkVersion3,-1)
  }

  @Test
  def testTransactionalUpdateLeaderAndIsr() {
    zkUtils.makeSurePersistentPathExists(ZkUtils.IsrChangeNotificationPath)
    zkUtils.makeSurePersistentPathExists(ZkUtils.ControllerEpochPath)

    val replicas = List(0,1)
    val newReplicas = List(1,2)
    val controllerContext = new ControllerContext(zkUtils)

    // regular update
    val newLeaderAndIsr1 = new LeaderAndIsr(brokerId, leaderEpoch, replicas, 0)
    val (updateSucceeded1,newZkVersion1) = ReplicationUtils.transactionalUpdateLeaderAndIsr(zkUtils,
      "my-topic-test", partitionId, newLeaderAndIsr1, 0, controllerContext.epoch, controllerContext.epochZkVersion)
    assertTrue(updateSucceeded1)
    assertEquals(newZkVersion1, 1)

    // mismatched zkVersion with the same data
    val newLeaderAndIsr2 = new LeaderAndIsr(brokerId, leaderEpoch, replicas, zkVersion + 1)
    val (updateSucceeded2,newZkVersion2) = ReplicationUtils.transactionalUpdateLeaderAndIsr(zkUtils,
      "my-topic-test", partitionId, newLeaderAndIsr2, zkVersion + 1, controllerContext.epoch, controllerContext.epochZkVersion)
    assertTrue(updateSucceeded2)
    // returns true with existing zkVersion
    assertEquals(newZkVersion2, 1)

    // Increment controller epoch
    val newControllerEpoch = controllerContext.epoch + 1
    val (updateSucceeded, newVersion) = zkUtils.conditionalUpdatePersistentPathIfExists(
      ZkUtils.ControllerEpochPath, newControllerEpoch.toString, controllerContext.epochZkVersion)
    assertTrue(updateSucceeded)
    assertEquals(1, newVersion)

    // Update LeaderAndIsr in zk using old controller epoch
    val newLeaderAndIsr3 = new LeaderAndIsr(brokerId, leaderEpoch + 1, newReplicas, zkVersion + 1)
    intercept[ControllerMovedException](ReplicationUtils.transactionalUpdateLeaderAndIsr(zkUtils,
      "my-topic-test", partitionId, newLeaderAndIsr3, zkVersion + 1, controllerContext.epoch, controllerContext.epochZkVersion))

    // The LeaderAndIsr should not be updated by the old controller
    val leaderAndIsrData = zkUtils.getLeaderAndIsrForPartition("my-topic-test", partitionId)
    assertEquals(leaderEpoch, leaderAndIsrData.get.leaderEpoch)
    assertEquals(replicas, leaderAndIsrData.get.isr)
    assertEquals(brokerId, leaderAndIsrData.get.leader)
    assertEquals(zkVersion, leaderAndIsrData.get.zkVersion)
  }

  @Test
  def testGetLeaderIsrAndEpochForPartition() {
    val leaderIsrAndControllerEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partitionId)
    assertEquals(topicDataLeaderIsrAndControllerEpoch, leaderIsrAndControllerEpoch.get)
    assertEquals(None, ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partitionId + 1))
  }

}
