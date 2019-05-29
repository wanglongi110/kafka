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

package kafka.server

import java.util.concurrent.locks.ReentrantLock

import kafka.cluster.{BrokerEndPoint}
import kafka.utils.{DelayedItem, Pool, ShutdownableThread}
import org.apache.kafka.common.errors.{CorruptRecordException, KafkaStorageException}
import kafka.common.ClientIdAndBroker
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.protocol.Errors
import ScalaConsumerAbstractFetcherThread._

import scala.collection.{Map, Set, mutable}
import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.internals.{FatalExitError, PartitionStates}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.EpochEndOffset

import scala.math._

/**
 *  AbstractFetcherThread from 2.0 release. Renaming it to ScalaConsumerAbstractFetcherThread and
 *  add it back to support scala consumer.
 *  Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class ScalaConsumerAbstractFetcherThread(name: String,
                                     clientId: String,
                                     val sourceBroker: BrokerEndPoint,
                                     fetchBackOffMs: Int = 0,
                                     isInterruptible: Boolean = true,
                                     includeLogTruncation: Boolean)
  extends ShutdownableThread(name, isInterruptible) {

  type REQ <: FetchRequest
  type PD <: PartitionData

  private[server] val partitionStates = new PartitionStates[ScalaConsumerPartitionFetchState]
  private val partitionMapLock = new ReentrantLock
  private val partitionMapCond = partitionMapLock.newCondition()

  private val metricId = ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port)
  val fetcherStats = new ScalaConsumerFetcherStats(metricId)
  val fetcherLagStats = new ScalaConsumerFetcherLagStats(metricId)
  var lastFetchTimeMs = System.currentTimeMillis
  @volatile var idle = false

  /* callbacks to be defined in subclass */

  // process fetched data
  protected def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: PD)

  // handle a partition whose offset is out of range and return a new fetch offset
  protected def handleOffsetOutOfRange(topicPartition: TopicPartition): Long

  // deal with partitions with errors, potentially due to leadership changes
  protected def handlePartitionsWithErrors(partitions: Iterable[TopicPartition])

  protected def buildLeaderEpochRequest(allPartitions: Seq[(TopicPartition, ScalaConsumerPartitionFetchState)]): ResultWithPartitions[Map[TopicPartition, Int]]

  protected def fetchEpochsFromLeader(partitions: Map[TopicPartition, Int]): Map[TopicPartition, EpochEndOffset]

  protected def maybeTruncate(fetchedEpochs: Map[TopicPartition, EpochEndOffset]): ResultWithPartitions[Map[TopicPartition, ScalaConsumerOffsetTruncationState]]

  protected def buildFetchRequest(partitionMap: Seq[(TopicPartition, ScalaConsumerPartitionFetchState)]): ResultWithPartitions[REQ]

  protected def fetch(fetchRequest: REQ): Seq[(TopicPartition, PD)]

  override def shutdown() {
    initiateShutdown()
    inLock(partitionMapLock) {
      partitionMapCond.signalAll()
    }
    awaitShutdown()

    // we don't need the lock since the thread has finished shutdown and metric removal is safe
    fetcherStats.unregister()
    fetcherLagStats.unregister()
  }

  private def states() = partitionStates.partitionStates.asScala.map { state => state.topicPartition -> state.value }

  override def doWork() {
    maybeTruncate()
    val fetchRequest = inLock(partitionMapLock) {
      val ResultWithPartitions(fetchRequest, partitionsWithError) = buildFetchRequest(states)
      if (fetchRequest.isEmpty) {
        trace(s"There are no active partitions. Back off for $fetchBackOffMs ms before sending a fetch request")
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }
      handlePartitionsWithErrors(partitionsWithError)
      fetchRequest
    }
    if (!fetchRequest.isEmpty)
      processFetchRequest(fetchRequest)
  }

  /**
    * - Build a leader epoch fetch based on partitions that are in the Truncating phase
    * - Issue LeaderEpochRequeust, retrieving the latest offset for each partition's
    *   leader epoch. This is the offset the follower should truncate to ensure
    *   accurate log replication.
    * - Finally truncate the logs for partitions in the truncating phase and mark them
    *   truncation complete. Do this within a lock to ensure no leadership changes can
    *   occur during truncation.
    */
  def maybeTruncate(): Unit = {
    val ResultWithPartitions(epochRequests, partitionsWithError) = inLock(partitionMapLock) { buildLeaderEpochRequest(states) }
    handlePartitionsWithErrors(partitionsWithError)

    if (epochRequests.nonEmpty) {
      val fetchedEpochs = fetchEpochsFromLeader(epochRequests)
      //Ensure we hold a lock during truncation.
      inLock(partitionMapLock) {
        //Check no leadership changes happened whilst we were unlocked, fetching epochs
        val leaderEpochs = fetchedEpochs.filter { case (tp, _) => partitionStates.contains(tp) }
        val ResultWithPartitions(fetchOffsets, partitionsWithError) = maybeTruncate(leaderEpochs)
        handlePartitionsWithErrors(partitionsWithError)
        updateFetchOffsetAndMaybeMarkTruncationComplete(fetchOffsets)
      }
    }
  }

  private def processFetchRequest(fetchRequest: REQ) {
    val partitionsWithError = mutable.Set[TopicPartition]()
    var responseData: Seq[(TopicPartition, PD)] = Seq.empty

    try {
      trace(s"Issuing fetch to broker ${sourceBroker.id}, request: $fetchRequest")
      lastFetchTimeMs = System.currentTimeMillis
      responseData = fetch(fetchRequest)
    } catch {
      case t: Throwable =>
        if (isRunning) {
          warn(s"Error in response for fetch request $fetchRequest", t)
          inLock(partitionMapLock) {
            partitionsWithError ++= partitionStates.partitionSet.asScala
            // there is an error occurred while fetching partitions, sleep a while
            // note that `ReplicaFetcherThread.handlePartitionsWithError` will also introduce the same delay for every
            // partition with error effectively doubling the delay. It would be good to improve this.
            partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
          }
        }
    }
    fetcherStats.requestRate.mark()

    if (responseData.nonEmpty) {
      // process fetched data
      inLock(partitionMapLock) {

        responseData.foreach { case (topicPartition, partitionData) =>
          val topic = topicPartition.topic
          val partitionId = topicPartition.partition
          Option(partitionStates.stateValue(topicPartition)).foreach(currentPartitionFetchState =>
            // It's possible that a partition is removed and re-added or truncated when there is a pending fetch request.
            // In this case, we only want to process the fetch response if the partition state is ready for fetch and the current offset is the same as the offset requested.
            if (fetchRequest.offset(topicPartition) == currentPartitionFetchState.fetchOffset &&
                currentPartitionFetchState.isReadyForFetch) {
              partitionData.error match {
                case Errors.NONE =>
                  try {
                    val records = partitionData.toRecords
                    val newOffset = records.batches.asScala.lastOption.map(_.nextOffset).getOrElse(
                      currentPartitionFetchState.fetchOffset)

                    fetcherLagStats.getAndMaybePut(topic, partitionId).lag = Math.max(0L, partitionData.highWatermark - newOffset)
                    // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                    processPartitionData(topicPartition, currentPartitionFetchState.fetchOffset, partitionData)

                    val validBytes = records.validBytes
                    // ReplicaDirAlterThread may have removed topicPartition from the partitionStates after processing the partition data
                    if (validBytes > 0 && partitionStates.contains(topicPartition)) {
                      // Update partitionStates only if there is no exception during processPartitionData
                      partitionStates.updateAndMoveToEnd(topicPartition, new ScalaConsumerPartitionFetchState(newOffset))
                      fetcherStats.byteRate.mark(validBytes)
                    }
                  } catch {
                    case ime: CorruptRecordException =>
                      // we log the error and continue. This ensures two things
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                      // should get fixed in the subsequent fetches
                      error(s"Found invalid messages during fetch for partition $topicPartition offset ${currentPartitionFetchState.fetchOffset}", ime)
                      partitionsWithError += topicPartition
                    case e: KafkaStorageException =>
                      error(s"Error while processing data for partition $topicPartition", e)
                      partitionsWithError += topicPartition
                    case e: Throwable =>
                      throw new KafkaException(s"Error processing data for partition $topicPartition " +
                        s"offset ${currentPartitionFetchState.fetchOffset}", e)
                  }
                case Errors.OFFSET_OUT_OF_RANGE =>
                  try {
                    val newOffset = handleOffsetOutOfRange(topicPartition)
                    partitionStates.updateAndMoveToEnd(topicPartition, new ScalaConsumerPartitionFetchState(newOffset))
                    info(s"Current offset ${currentPartitionFetchState.fetchOffset} for partition $topicPartition is " +
                      s"out of range, which typically implies a leader change. Reset fetch offset to $newOffset")
                  } catch {
                    case e: FatalExitError => throw e
                    case e: Throwable =>
                      error(s"Error getting offset for partition $topicPartition", e)
                      partitionsWithError += topicPartition
                  }

                case Errors.NOT_LEADER_FOR_PARTITION =>
                  info(s"Remote broker is not the leader for partition $topicPartition, which could indicate " +
                    "that the partition is being moved")
                  partitionsWithError += topicPartition

                case _ =>
                  error(s"Error for partition $topicPartition at offset ${currentPartitionFetchState.fetchOffset}",
                    partitionData.exception.get)
                  partitionsWithError += topicPartition
              }
            })
        }
      }
    }

    if (partitionsWithError.nonEmpty) {
      debug(s"Handling errors for partitions $partitionsWithError")
      handlePartitionsWithErrors(partitionsWithError)
    }
  }

  def markPartitionsForTruncation(topicPartition: TopicPartition, truncationOffset: Long) {
    if (!includeLogTruncation)
      throw new IllegalStateException("Truncation should not be requested if includeLogTruncation is disabled")
    partitionMapLock.lockInterruptibly()
    try {
      Option(partitionStates.stateValue(topicPartition)).foreach { state =>
        val newState = ScalaConsumerPartitionFetchState(math.min(truncationOffset, state.fetchOffset), state.delay, truncatingLog = true)
        partitionStates.updateAndMoveToEnd(topicPartition, newState)
      }
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  def addPartitions(initialFetchOffsets: Map[TopicPartition, Long]) {
    partitionMapLock.lockInterruptibly()
    try {
      // If the partitionMap already has the topic/partition, then do not update the map with the old offset
      val newPartitionToState = initialFetchOffsets.filter { case (tp, _) =>
        !partitionStates.contains(tp)
      }.map { case (tp, initialFetchOffset) =>
        val fetchState =
          if (initialFetchOffset < 0)
            new ScalaConsumerPartitionFetchState(handleOffsetOutOfRange(tp), includeLogTruncation)
          else
            new ScalaConsumerPartitionFetchState(initialFetchOffset, includeLogTruncation)
        tp -> fetchState
      }
      val existingPartitionToState = states().toMap
      partitionStates.set((existingPartitionToState ++ newPartitionToState).asJava)
      maybeUpdateIdleFlag()
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  /**
    * Loop through all partitions, updating their fetch offset and maybe marking them as
    * truncation completed if their offsetTruncationState indicates truncation completed
    *
    * @param fetchOffsets the partitions to update fetch offset and maybe mark truncation complete
    */
  private def updateFetchOffsetAndMaybeMarkTruncationComplete(fetchOffsets: Map[TopicPartition, ScalaConsumerOffsetTruncationState]) {
    val newStates: Map[TopicPartition, ScalaConsumerPartitionFetchState] = partitionStates.partitionStates.asScala
      .map { state =>
        val maybeTruncationComplete = fetchOffsets.get(state.topicPartition()) match {
          case Some(offsetTruncationState) => ScalaConsumerPartitionFetchState(offsetTruncationState.offset, state.value.delay, truncatingLog = !offsetTruncationState.truncationCompleted)
          case None => state.value()
        }
        (state.topicPartition(), maybeTruncationComplete)
      }.toMap
    partitionStates.set(newStates.asJava)
    maybeUpdateIdleFlag()
  }

  def delayPartitions(partitions: Iterable[TopicPartition], delay: Long) {
    partitionMapLock.lockInterruptibly()
    try {
      for (partition <- partitions) {
        Option(partitionStates.stateValue(partition)).foreach (currentPartitionFetchState =>
          if (!currentPartitionFetchState.isDelayed)
            partitionStates.updateAndMoveToEnd(partition, ScalaConsumerPartitionFetchState(currentPartitionFetchState.fetchOffset, new DelayedItem(delay), currentPartitionFetchState.truncatingLog))
        )
      }
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  def removePartitions(topicPartitions: Set[TopicPartition]) {
    partitionMapLock.lockInterruptibly()
    try {
      topicPartitions.foreach { topicPartition =>
        partitionStates.remove(topicPartition)
        fetcherLagStats.unregister(topicPartition.topic, topicPartition.partition)
      }
      maybeUpdateIdleFlag()
    } finally partitionMapLock.unlock()
  }

  private[server] def partitionsAndOffsets: Map[TopicPartition, BrokerAndInitialOffset] = inLock(partitionMapLock) {
    partitionStates.partitionStates.asScala.map { case state =>
      state.topicPartition -> new BrokerAndInitialOffset(sourceBroker, state.value.fetchOffset)
    }.toMap
  }

  // This method should only be called when holding the partitionMapLock
  private def maybeUpdateIdleFlag(): Unit = {
    idle = partitionStates.size() <= 0
  }

}

object ScalaConsumerAbstractFetcherThread {

  case class ResultWithPartitions[R](result: R, partitionsWithError: Set[TopicPartition])

  trait FetchRequest {
    def isEmpty: Boolean
    def offset(topicPartition: TopicPartition): Long
  }

  trait PartitionData {
    def error: Errors
    def exception: Option[Throwable]
    def toRecords: MemoryRecords
    def highWatermark: Long
  }

}

object ScalaConsumerFetcherMetrics {
  val ConsumerLag = "ConsumerLag"
  val RequestsPerSec = "RequestsPerSec"
  val BytesPerSec = "BytesPerSec"
}

class ScalaConsumerFetcherLagMetrics(metricId: ScalaConsumerClientIdTopicPartition) extends KafkaMetricsGroup {

  private[this] val lagVal = new AtomicLong(-1L)
  private[this] val tags = Map(
    "clientId" -> metricId.clientId,
    "topic" -> metricId.topic,
    "partition" -> metricId.partitionId.toString)

  newGauge(ScalaConsumerFetcherMetrics.ConsumerLag,
    new Gauge[Long] {
      def value = lagVal.get
    },
    tags
  )

  def lag_=(newLag: Long) {
    lagVal.set(newLag)
  }

  def lag = lagVal.get

  def unregister() {
    removeMetric(ScalaConsumerFetcherMetrics.ConsumerLag, tags)
  }
}

class ScalaConsumerFetcherLagStats(metricId: ClientIdAndBroker) {
  private val valueFactory = (k: ScalaConsumerClientIdTopicPartition) => new ScalaConsumerFetcherLagMetrics(k)
  val stats = new Pool[ScalaConsumerClientIdTopicPartition, ScalaConsumerFetcherLagMetrics](Some(valueFactory))

  def getAndMaybePut(topic: String, partitionId: Int): ScalaConsumerFetcherLagMetrics = {
    stats.getAndMaybePut(ScalaConsumerClientIdTopicPartition(metricId.clientId, topic, partitionId))
  }

  def isReplicaInSync(topic: String, partitionId: Int): Boolean = {
    val fetcherLagMetrics = stats.get(ScalaConsumerClientIdTopicPartition(metricId.clientId, topic, partitionId))
    if (fetcherLagMetrics != null)
      fetcherLagMetrics.lag <= 0
    else
      false
  }

  def unregister(topic: String, partitionId: Int) {
    val lagMetrics = stats.remove(ScalaConsumerClientIdTopicPartition(metricId.clientId, topic, partitionId))
    if (lagMetrics != null) lagMetrics.unregister()
  }

  def unregister() {
    stats.keys.toBuffer.foreach { key: ScalaConsumerClientIdTopicPartition =>
      unregister(key.topic, key.partitionId)
    }
  }
}

class ScalaConsumerFetcherStats(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val tags = Map("clientId" -> metricId.clientId,
    "brokerHost" -> metricId.brokerHost,
    "brokerPort" -> metricId.brokerPort.toString)

  val requestRate = newMeter(ScalaConsumerFetcherMetrics.RequestsPerSec, "requests", TimeUnit.SECONDS, tags)

  val byteRate = newMeter(ScalaConsumerFetcherMetrics.BytesPerSec, "bytes", TimeUnit.SECONDS, tags)

  def unregister() {
    removeMetric(ScalaConsumerFetcherMetrics.RequestsPerSec, tags)
    removeMetric(ScalaConsumerFetcherMetrics.BytesPerSec, tags)
  }

}

case class ScalaConsumerClientIdTopicPartition(clientId: String, topic: String, partitionId: Int) {
  override def toString = "%s-%s-%d".format(clientId, topic, partitionId)
}

/**
  * case class to keep partition offset and its state(truncatingLog, delayed)
  * This represents a partition as being either:
  * (1) Truncating its log, for example having recently become a follower
  * (2) Delayed, for example due to an error, where we subsequently back off a bit
  * (3) ReadyForFetch, the is the active state where the thread is actively fetching data.
  */
case class ScalaConsumerPartitionFetchState(fetchOffset: Long, delay: DelayedItem, truncatingLog: Boolean = false) {

  def this(offset: Long, truncatingLog: Boolean) = this(offset, new DelayedItem(0), truncatingLog)

  def this(offset: Long, delay: DelayedItem) = this(offset, delay, false)

  def this(fetchOffset: Long) = this(fetchOffset, new DelayedItem(0))

  def isReadyForFetch: Boolean = delay.getDelay(TimeUnit.MILLISECONDS) == 0 && !truncatingLog

  def isTruncatingLog: Boolean = delay.getDelay(TimeUnit.MILLISECONDS) == 0 && truncatingLog

  def isDelayed: Boolean = delay.getDelay(TimeUnit.MILLISECONDS) > 0

  override def toString = "offset:%d-isReadyForFetch:%b-isTruncatingLog:%b".format(fetchOffset, isReadyForFetch, truncatingLog)
}

case class ScalaConsumerOffsetTruncationState(offset: Long, truncationCompleted: Boolean) {

  def this(offset: Long) = this(offset, true)

  override def toString = "offset:%d-truncationCompleted:%b".format(offset, truncationCompleted)
}
