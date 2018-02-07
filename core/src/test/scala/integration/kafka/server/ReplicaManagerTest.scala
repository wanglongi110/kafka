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
package integration.kafka.server

import java.util.concurrent.atomic.AtomicLong

import kafka.server.KafkaConfig
import kafka.utils.{TestUtils, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time
import org.junit.Test
import org.junit.Assert._

class ReplicaManagerTest extends ZooKeeperTestHarness {
  @Test
  def testMaybePropagateIsrChanges(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, zkConnect))
    val server = TestUtils.createServer(config, new Time {
      private val _clock = new AtomicLong(1000)
      override def milliseconds(): Long = _clock.getAndAdd(10000)
      override def hiResClockMs(): Long = _clock.getAndAdd(10000)
      override def sleep(ms: Long): Unit = {}
      override def nanoseconds(): Long = _clock.getAndAdd(10000) * 1000 * 1000
    })

    // Topic name which is the maximum length of 249.
    val largeTopicName = ("topic-" padTo(249, "x")).mkString

    // 5000 partitions. Large partition numbers chosen to make the serialized values as long as possible.
    (1000000000 to 1000005000)
      .map(n => new TopicPartition(largeTopicName, n))
      .foreach(server.replicaManager.recordIsrChange)

    server.replicaManager.maybePropagateIsrChanges()

    val isrChangeNotificationQueuePath = "/isr_change_notification"
    val node0 = "isr_change_0000000000"
    val node1 = "isr_change_0000000001"

    val zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkAclsEnabled.getOrElse(JaasUtils.isZkSecurityEnabled()))
    assertEquals(
      zkUtils.getChildren(isrChangeNotificationQueuePath).toSet,
      Set(node0, node1))

    val (_, stat0) = zkUtils.readData(isrChangeNotificationQueuePath + "/" + node0)
    val (_, stat1) = zkUtils.readData(isrChangeNotificationQueuePath + "/" + node1)

    assertEquals(912028, stat0.getDataLength)
    assertEquals(513313, stat1.getDataLength)

    zkUtils.close()
    server.shutdown()
    server.awaitShutdown()
  }
}
