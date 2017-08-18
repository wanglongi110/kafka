/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.consumer

import java.util
import java.util.Optional

import kafka.api.OffsetRequest
import kafka.cluster.BrokerEndPoint
import kafka.consumer.SSLConsumerFetcherThread.{FetchRequest, PartitionData}
import kafka.message.ByteBufferMessageSet
import kafka.server.ScalaConsumerAbstractFetcherThread.ResultWithPartitions
import kafka.server._
import org.apache.kafka.clients.ManualMetadataUpdater
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.requests.{EpochEndOffset, FetchResponse, IsolationLevel, ListOffsetRequest, ListOffsetResponse, FetchRequest => JFetchRequest}
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.Map

class SSLConsumerFetcherThread(consumerIdString: String,
                               fetcherId: Int,
                               val config: ConsumerConfig,
                               sourceBroker: BrokerEndPoint,
                               partitionMap: Map[TopicPartition, PartitionTopicInfo],
                               val consumerFetcherManager: ConsumerFetcherManager)
              extends ScalaConsumerAbstractFetcherThread(name = s"ConsumerFetcherThread-$consumerIdString-$fetcherId-${sourceBroker.id}",
                                            clientId = config.clientId,
                                            sourceBroker = sourceBroker,
                                            fetchBackOffMs = config.refreshLeaderBackoffMs,
                                            isInterruptible = true,
                                            includeLogTruncation = false) {

  type REQ = FetchRequest
  type PD = PartitionData

  private val clientId = config.clientId
  private val fetchSize = config.fetchMessageMaxBytes

  private val sourceNode = new Node(sourceBroker.id, sourceBroker.host, sourceBroker.port)
  val time = new SystemTime
  val socketTimeout = config.socketTimeoutMs

  private val networkClient = new SSLNetworkClient(config, new ManualMetadataUpdater())

  override def initiateShutdown(): Boolean = {
    val justShutdown = super.initiateShutdown()
    justShutdown
  }

  override def shutdown(): Unit = {
    super.shutdown()
    networkClient.close()
  }

  // process fetched data
  def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: PartitionData) {
    val pti = partitionMap(topicPartition)
    if (pti.getFetchOffset != fetchOffset)
      throw new RuntimeException("Offset doesn't match for partition [%s,%d] pti offset: %d fetch offset: %d"
        .format(topicPartition.topic, topicPartition.partition, pti.getFetchOffset, fetchOffset))
    pti.enqueue(new ByteBufferMessageSet(partitionData.toRecords.buffer()))
  }

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topicPartition: TopicPartition): Long = {
    val startTimestamp = config.autoOffsetReset match {
      case OffsetRequest.SmallestTimeString => OffsetRequest.EarliestTime
      case _ => OffsetRequest.LatestTime
    }
    val newOffset = earliestOrLatestOffset(topicPartition, startTimestamp, kafka.api.Request.OrdinaryConsumerId)
    val pti = partitionMap(topicPartition)
    pti.resetFetchOffset(newOffset)
    pti.resetConsumeOffset(newOffset)
    newOffset
  }

  // any logic for partitions whose leader has changed
  def handlePartitionsWithErrors(partitions: Iterable[TopicPartition]) {
    removePartitions(partitions.toSet)
    consumerFetcherManager.addPartitionsWithError(partitions)
  }

  private def earliestOrLatestOffset(topicPartition: TopicPartition, earliestOrLatest: Long, consumerId: Int): Long = {
    val request = {
      val partitions = Map(topicPartition ->
        new ListOffsetRequest.PartitionData(java.lang.Long.valueOf(earliestOrLatest), Optional.empty[Integer]()))
      ListOffsetRequest.Builder.forConsumer(false, IsolationLevel.READ_UNCOMMITTED).setTargetTimes(partitions.asJava)
    }
    val clientResponse = networkClient.sendRequest(sourceNode, ApiKeys.LIST_OFFSETS, request)
    val response = clientResponse.responseBody.asInstanceOf[ListOffsetResponse]
    val partitionData = response.responseData.get(topicPartition)
    Errors.forCode(partitionData.error.code()) match {
      case Errors.NONE =>
        partitionData.offset
      case errorCode => throw errorCode.exception
    }
  }

  protected def buildFetchRequest(partitionMap: collection.Seq[(TopicPartition, ScalaConsumerPartitionFetchState)]): ResultWithPartitions[FetchRequest] = {
    val requestMap = new util.LinkedHashMap[TopicPartition, JFetchRequest.PartitionData]
    partitionMap.foreach { case (topicPartition, partitionFetchState) =>
      if (!partitionFetchState.isDelayed)
        requestMap.put(topicPartition, new JFetchRequest.PartitionData(partitionFetchState.fetchOffset, JFetchRequest.INVALID_LOG_START_OFFSET, fetchSize, Optional.empty()))
    }

    val version: java.lang.Short = new java.lang.Short("3")
    val requestBuilder = JFetchRequest.Builder.forConsumer(version, config.fetchWaitMaxMs, config.fetchMinBytes, requestMap).setMaxBytes(config.fetchMaxBytes)
    ResultWithPartitions(new FetchRequest(requestBuilder), Set())
  }

  protected def fetch(fetchRequest: FetchRequest): Seq[(TopicPartition, PartitionData)] = {
    val clientResponse = networkClient.sendRequest(sourceNode, ApiKeys.FETCH, fetchRequest.underlying)
    clientResponse.responseBody.asInstanceOf[FetchResponse[Records]].responseData.asScala.toSeq.map {
      case (key, value) =>
      key -> new PartitionData(value)
    }
  }

  override def buildLeaderEpochRequest(allPartitions: Seq[(TopicPartition, ScalaConsumerPartitionFetchState)]): ResultWithPartitions[Map[TopicPartition, Int]] = { ResultWithPartitions(Map(), Set()) }

  override def fetchEpochsFromLeader(partitions: Map[TopicPartition, Int]): Map[TopicPartition, EpochEndOffset] = { Map() }

  override def maybeTruncate(fetchedEpochs: Map[TopicPartition, EpochEndOffset]): ResultWithPartitions[Map[TopicPartition, ScalaConsumerOffsetTruncationState]] = { ResultWithPartitions(Map(), Set()) }
}

object SSLConsumerFetcherThread {

  class FetchRequest(val underlying: JFetchRequest.Builder) extends ScalaConsumerAbstractFetcherThread.FetchRequest {
    def isEmpty: Boolean = underlying.fetchData().isEmpty
    def offset(topicPartition: TopicPartition): Long =
      underlying.fetchData().asScala(topicPartition).fetchOffset
  }

  class PartitionData(val underlying: FetchResponse.PartitionData[Records]) extends ScalaConsumerAbstractFetcherThread.PartitionData {
    def error: Errors = underlying.error
    def toRecords: MemoryRecords = underlying.records.asInstanceOf[MemoryRecords]
    def highWatermark: Long = underlying.highWatermark
    def exception: Option[Throwable] = error match {
      case Errors.NONE => None
      case e => Some(e.exception)
    }

  }
}
