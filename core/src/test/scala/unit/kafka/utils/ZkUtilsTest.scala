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

import kafka.common.TopicAndPartition
import kafka.zk.ZooKeeperTestHarness
import org.I0Itec.zkclient.exception.ZkException
import org.apache.kafka.common.errors.ControllerMovedException
import org.junit.Assert._
import org.junit.Test

import scala.collection._


class ZkUtilsTest extends ZooKeeperTestHarness {

  val path = "/path"

  @Test
  def testSuccessfulConditionalDeletePath() {
    // Given an existing path
    zkUtils.createPersistentPath(path)
    val (_, statAfterCreation) = zkUtils.readData(path)

    // Deletion is successful when the version number matches
    assertTrue("Deletion should be successful", zkUtils.conditionalDeletePath(path, statAfterCreation.getVersion))
    val (optionalData, _) = zkUtils.readDataMaybeNull(path)
    assertTrue("Node should be deleted", optionalData.isEmpty)

    // Deletion is successful when the node does not exist too
    assertTrue("Deletion should be successful", zkUtils.conditionalDeletePath(path, 0))
  }

  // Verify behaviour of ZkUtils.createSequentialPersistentPath since PIDManager relies on it
  @Test
  def testPersistentSequentialPath() {
    // Given an existing path
    zkUtils.createPersistentPath(path)

    var result = zkUtils.createSequentialPersistentPath(path + "/sequence_")

    assertEquals("/path/sequence_0000000000", result)

    result = zkUtils.createSequentialPersistentPath(path + "/sequence_")

    assertEquals("/path/sequence_0000000001", result)
  }

  @Test
  def testAbortedConditionalDeletePath() {
    // Given an existing path that gets updated
    zkUtils.createPersistentPath(path)
    val (_, statAfterCreation) = zkUtils.readData(path)
    zkUtils.updatePersistentPath(path, "data")

    // Deletion is aborted when the version number does not match
    assertFalse("Deletion should be aborted", zkUtils.conditionalDeletePath(path, statAfterCreation.getVersion))
    val (optionalData, _) = zkUtils.readDataMaybeNull(path)
    assertTrue("Node should still be there", optionalData.isDefined)
  }

  @Test
  def testClusterIdentifierJsonParsing() {
    val clusterId = "test"
    assertEquals(zkUtils.ClusterId.fromJson(zkUtils.ClusterId.toJson(clusterId)), clusterId)
  }

  @Test
  def testGetAllPartitionsTopicWithoutPartitions() {
    val topic = "testtopic"
    // Create a regular topic and a topic without any partitions
    zkUtils.createPersistentPath(ZkUtils.getTopicPartitionPath(topic, 0))
    zkUtils.createPersistentPath(ZkUtils.getTopicPath("nopartitions"))

    assertEquals(Set(TopicAndPartition(topic, 0)), zkUtils.getAllPartitions())
  }

  @Test
  def testCheckAndDeleteMultiOps() {
    val checkVersion = 0
    val deletePath = "/delete/state"
    zkUtils.createPersistentPath(ZkUtils.ControllerEpochPath, "somedata")
    zkUtils.makeSurePersistentPathExists(deletePath)

    intercept[ControllerMovedException](zkUtils.transactionalDeletePath(checkVersion + 1, deletePath))

    assertTrue(zkUtils.pathExists(deletePath))

    zkUtils.transactionalDeletePath(checkVersion, deletePath)

    assertFalse(zkUtils.pathExists(deletePath))
  }

  @Test
  def testCheckAndDeleteRecursiveMultiOps() {
    val checkVersion = 0
    val deletePaths = Seq(
      "/delete/state",
      "/delete/state/child-1",
      "/delete/state/child-2",
      "/delete/state/child-1/grandchild-1",
      "/delete/state/child-1/grandchild-2")
    zkUtils.createPersistentPath(ZkUtils.ControllerEpochPath, "somedata")
    deletePaths.foreach(zkUtils.makeSurePersistentPathExists(_))

    intercept[ControllerMovedException](zkUtils.transactionalDeletePathRecursive(checkVersion+1, deletePaths.head))

    // Delete path (non-recursive) should fail if there are children znodes
    var fail = false
    try {
      zkUtils.transactionalDeletePath(checkVersion, deletePaths.head)
    } catch {
      case _: ZkException => fail = true
    }
    assertTrue(fail)

    deletePaths.foreach(e => assertTrue(zkUtils.pathExists(e)))

    zkUtils.transactionalDeletePathRecursive(checkVersion, deletePaths.head)

    deletePaths.foreach(e => assertFalse(zkUtils.pathExists(e)))
  }

  @Test
  def testCheckAndUpdateMultiOps() {
    val checkVersion = 0
    var fail = false
    val updatePath = "/delete/state"
    zkUtils.createPersistentPath(ZkUtils.ControllerEpochPath, "somedata")
    zkUtils.createPersistentPath(updatePath, "somedata")

    intercept[ControllerMovedException](zkUtils.transactionalUpdatePersistentPath(checkVersion+1, updatePath, "somedata-2"))

    assertEquals("somedata", zkUtils.readData(updatePath)._1)

    zkUtils.transactionalUpdatePersistentPath(checkVersion, updatePath, "somedata-2")

    assertEquals("somedata-2", zkUtils.readData(updatePath)._1)
  }

  @Test
  def testCheckAndCreateMultiOps() {
    val checkVersion = 0
    val createPath = "/delete/a/b/c/state"
    zkUtils.createPersistentPath(ZkUtils.ControllerEpochPath, "somedata")

    intercept[ControllerMovedException](zkUtils.transactionalCreatePersistentPath(checkVersion+1, createPath, "somedata"))

    assertFalse(zkUtils.pathExists(createPath))

    zkUtils.transactionalCreatePersistentPath(checkVersion, createPath, "somedata")

    assertEquals("somedata", zkUtils.readData(ZkUtils.ControllerEpochPath)._1)
  }
}
