/**
 *
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

package kafka.consumer

import java.math.BigInteger
import java.security.MessageDigest

import kafka.common.TopicAndPartition

object PartitionAssignmentStrategies {
  private val fromName = Map(
    RangeStrategy.name -> RangeStrategy,
    SymmetricStrategy.name -> SymmetricStrategy,
    RoundRobinStrategy.name -> RoundRobinStrategy,
    RoundRobinV1Strategy.name -> RoundRobinV1Strategy,
    RoundRobinV2Strategy.name -> RoundRobinV2Strategy
  )

  val validStrategies = fromName.map(_._1.toString).mkString(",")

  def apply(name: String) = fromName(name)

  def isValid(name: String) = fromName.contains(name)

  sealed trait Strategy {
    val name: String

    override def toString = name
  }

  sealed trait RoundRobinVariant extends Strategy {
    val partitionOrder: (TopicAndPartition, TopicAndPartition) => Boolean
  }

  case object RangeStrategy extends Strategy {
    val name = "range"
  }

  case object SymmetricStrategy extends RoundRobinVariant {
    val name = "symmetric"
    val partitionOrder = (topicPartition1: TopicAndPartition, topicPartition2: TopicAndPartition) => {
      if (topicPartition1.partition == topicPartition2.partition)
        topicPartition1.topic < topicPartition2.topic
      else
        topicPartition1.partition < topicPartition2.partition
    }
  }

  case object RoundRobinStrategy extends RoundRobinVariant {
    val name = "roundrobin"
    val partitionOrder = (topicPartition1: TopicAndPartition, topicPartition2: TopicAndPartition) => {
      // this was the original round-robin order and does not work very well.
      // We are keeping it here for backwards compatibility
      topicPartition1.toString.hashCode < topicPartition2.toString.hashCode
    }
  }

  case object RoundRobinV1Strategy extends RoundRobinVariant {
    val name = "roundrobinv1"
    val partitionOrder = RoundRobinStrategy.partitionOrder
  }

  case object RoundRobinV2Strategy extends RoundRobinVariant {
    val name = "roundrobinv2"

    val partitionOrder = (topicPartition1: TopicAndPartition, topicPartition2: TopicAndPartition) => {
      val digestMd5 = MessageDigest.getInstance("MD5")
      val md5ForTopicPartition1 = new BigInteger(digestMd5.digest(topicPartition1.toString.getBytes("UTF-8"))).longValue()
      val md5ForTopicPartition2 = new BigInteger(digestMd5.digest(topicPartition2.toString.getBytes("UTF-8"))).longValue()
      md5ForTopicPartition1 < md5ForTopicPartition2
    }
  }
}

