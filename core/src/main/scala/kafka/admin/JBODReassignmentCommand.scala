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

package kafka.admin

import scala.collection.{JavaConversions, Map, Seq}
import kafka.utils.{CommandLineUtils, Json, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.security.JaasUtils
import joptsimple._
import kafka.zk.KafkaZkClient
import kafka.zookeeper.ZooKeeperClient
import org.apache.kafka.common.utils.Time


object JBODReassignmentCommand {

  def main(args: Array[String]): Unit = {
    val opts = new JBODReassignmentCommandOptions(args)
    val actions = Seq(opts.skipUrpOpt, opts.reassignOpt).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --skipUrp or --reassign")

    val zkConnect = opts.options.valueOf(opts.zkConnectOpt)
    val zkUtils = ZkUtils(zkConnect,
                          30000,
                          30000,
                          JaasUtils.isZkSecurityEnabled)
    val time = Time.SYSTEM
    var zkClient: KafkaZkClient = KafkaZkClient(zkConnect, JaasUtils.isZkSecurityEnabled, 30000, 30000, Int.MaxValue, time)

    try {
      if(opts.options.has(opts.skipUrpOpt))
        skipUrp(zkUtils, opts)
      else if(opts.options.has(opts.reassignOpt))
        executeReassignment(zkUtils, zkClient, opts)
    } catch {
      case e: Throwable =>
        println("Operation failed due to " + e.getMessage)
    } finally {
      zkUtils.close()
    }
  }

  def skipUrp(zkUtils: ZkUtils, opts: JBODReassignmentCommandOptions): Unit = {
    // Wait for hosts on a given rack to start shutting down
    println(s"Waiting for hosts on a given rack to start shutting down")
    val rack = waitAndGetTheShuttingDownRack(zkUtils)
    println(s"Hosts on the rack $rack are shutting down")

    // Allow all brokers on this rack to skip URP check in the upcoming hour
    println(s"Update ZK path /urp_verification_timestamp/$rack to allow brokers on the rack $rack to skip URP check in the next 10 minutes")
    zkUtils.updatePersistentPath("/urp_verification_timestamp/" + rack, (System.currentTimeMillis + 60000).toString)
  }

  def executeReassignment(zkUtils: ZkUtils, zkClient: KafkaZkClient, opts: JBODReassignmentCommandOptions): Unit = {
    val targetHostId = opts.options.valueOf(opts.hostIdOpt)
    val batchSize = opts.options.valueOf(opts.batchSizeOpt)
    val newPartitionAssignment = collection.mutable.Map.empty[TopicPartition, Seq[Int]]

    while (zkUtils.getPartitionsBeingReassigned().nonEmpty) {
      val partitions = zkUtils.getPartitionsBeingReassigned().keys
      println(s"${partitions.size} partitions are being re-assigned: $partitions\n")
      Thread.sleep(5000)
    }

    // Generate assignment
    val allTopics = zkUtils.getAllTopics().sorted
    for (topic <- allTopics) {
      zkUtils.getPartitionAssignmentForTopics(List(topic)).get(topic) match {
        case Some(topicPartitionAssignment) =>
          val sortedPartitions = topicPartitionAssignment.toSeq.sortBy(_._1)

          for ((partitionId, assignedReplicas) <- sortedPartitions) {
            val convertedAssignedReplicas = assignedReplicas.map(_.toString).map { brokerId =>
              val hostId = brokerId.substring(0, brokerId.length - 3)
              if (hostId == targetHostId)
                targetHostId + "001"
              else
                brokerId
            }.map(_.toInt)

            if (convertedAssignedReplicas.toSet.size < assignedReplicas.size)
              throw new RuntimeException(s"The replica list ${assignedReplicas.mkString(",")} for $topic-$partitionId has two or more replicas on the host $targetHostId")

            if (!convertedAssignedReplicas.equals(assignedReplicas)) {
              newPartitionAssignment.put(new TopicPartition(topic, partitionId), convertedAssignedReplicas)
            }
          }

        case None =>
          throw new RuntimeException(s"Topic $topic does not exist. It should not be possible.")
      }
    }

    println(s"There are ${newPartitionAssignment.size} partitions to be reassigned on host $targetHostId")

    // Execute assignment
    var remaining = newPartitionAssignment.size
    newPartitionAssignment.toSeq.grouped(batchSize).foreach { assignment =>
      while (zkUtils.getPartitionsBeingReassigned().nonEmpty) {
        val partitions = zkUtils.getPartitionsBeingReassigned().keys
        println(s"$remaining partitions are waiting to be reassigned. ${partitions.size} partitions are being re-assigned: $partitions\n")
        Thread.sleep(5000)
      }
      remaining -= assignment.size

      ReassignPartitionsCommand.executeAssignment(zkClient, None, formatAsJson(assignment), ReassignPartitionsCommand.Throttle(-1))
    }
  }


  private def waitAndGetTheShuttingDownRack(zkUtils: ZkUtils): String = {
    while (true) {
      val racks = zkUtils.getChildrenParentMayNotExist("/broker_restart_lock").map(znode => znode.substring(0, znode.lastIndexOf('_'))).toSet
      if (racks.size == 1) {
        return racks.head
      }
      System.out.println(s"Racks are $racks");
      Thread.sleep(1000)
    }
    throw new IllegalStateException("This state should not be reached")
  }

  private def formatAsJson(newPartitionAssignment: Seq[(TopicPartition, Seq[Int])]): String = {
    Json.encodeAsString(Map(
      "version" -> 1,
      "partitions" -> newPartitionAssignment.map { case (tp, replicas) =>
        Map(
          "topic" -> tp.topic(),
          "partition" -> tp.partition(),
          "replicas" -> replicas
        )
      }
    ))
  }

  class JBODReassignmentCommandOptions(args: Array[String]) {
    val parser = new OptionParser(false)
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the " +
      "form host:port. Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])
    val hostIdOpt = parser.accepts("hostId", "A hostId that the brokerId excluding the three-digit instance id")
      .withRequiredArg
      .describedAs("Host Id")
      .ofType(classOf[String])
    val batchSizeOpt = parser.accepts("batchSize", "The number of partitions to be reassigned at a time")
      .withRequiredArg()
      .describedAs("Batch size")
      .ofType(classOf[Int])
      .defaultsTo(100)
    val skipUrpOpt = parser.accepts("skipUrp", "All brokers on the first shutting down rack to skip URP check in the next hour")
    val reassignOpt = parser.accepts("reassign", "Kick off the reassignment to move all replicas on this host to the broker i001 on this host.")

    val options = parser.parse(args : _*)
    CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt, hostIdOpt)
  }
}
