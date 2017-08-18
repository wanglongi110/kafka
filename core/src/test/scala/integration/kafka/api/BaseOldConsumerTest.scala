/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package kafka.api

import integration.kafka.api.OldConsumerIntegrationTestHarness
import kafka.consumer.{ConsumerConnector, ConsumerTimeoutException}
import kafka.message.MessageAndMetadata
import kafka.server.KafkaConfig
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * Integration tests for the new consumer that cover basic usage as well as server failures
 */
abstract class BaseOldConsumerTest extends OldConsumerIntegrationTestHarness with Logging {

  val producerCount = 1
  val serverCount = 3
  val consumerCount = 1

  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)
  val part2 = 1
  val tp2 = new TopicPartition(topic, part2)

  // configure the servers and clients
  this.serverConfig.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false") // speed up shutdown
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "3") // don't want to lose offset
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")

  this.consumerConfig.setProperty("group.id", "my-test")
  this.consumerConfig.setProperty("auto.offset.reset", "earliest")
  this.consumerConfig.setProperty("auto.commit.enable", "false")
  this.consumerConfig.setProperty("consumer.timeout.ms", "3000")

  @Before
  override def setUp() {
    super.setUp()

    // create the test topic with all the brokers as replicas
    TestUtils.createTopic(this.zkClient, topic, 2, serverCount, this.servers)
  }

  @Test
  def testSimpleConsumption() {
    val numRecords = 10000
    sendRecords(numRecords)

    consumeAndVerifyRecords(consumer = this.consumers.head, numRecords = numRecords, startingOffset = 0)
  }

  protected def sendRecords(numRecords: Int): Seq[ProducerRecord[Array[Byte], Array[Byte]]] =
    sendRecords(numRecords, tp)

  protected def sendRecords(numRecords: Int, tp: TopicPartition): Seq[ProducerRecord[Array[Byte], Array[Byte]]] =
    sendRecords(this.producers.head, numRecords, tp)

  protected def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int,
                            tp: TopicPartition): Seq[ProducerRecord[Array[Byte], Array[Byte]]] = {
    val records = (0 until numRecords).map { i =>
      val record = new ProducerRecord(tp.topic(), tp.partition(), i.toLong, s"key $i".getBytes, s"value $i".getBytes)
      producer.send(record)
      record
    }
    producer.flush()

    records
  }

  protected def consumeAndVerifyRecords(consumer: ConsumerConnector,
                                        numRecords: Int,
                                        startingOffset: Int,
                                        startingKeyAndValueIndex: Int = 0,
                                        startingTimestamp: Long = 0L,
                                        timestampType: TimestampType = TimestampType.CREATE_TIME,
                                        tp: TopicPartition = tp,
                                        maxPollRecords: Int = Int.MaxValue) {
    val records = consumeRecords(consumer, numRecords, maxPollRecords = maxPollRecords)
    val now = System.currentTimeMillis()
    for (i <- 0 until numRecords) {
      val record = records(i)
      val offset = startingOffset + i
      assertEquals(tp.topic, record.topic)
      assertEquals(tp.partition, record.partition)
      if (timestampType == TimestampType.CREATE_TIME) {
        assertEquals(timestampType, record.timestampType)
        val timestamp = startingTimestamp + i
        assertEquals(timestamp.toLong, record.timestamp)
      } else
        assertTrue(s"Got unexpected timestamp ${record.timestamp}. Timestamp should be between [$startingTimestamp, $now}]",
          record.timestamp >= startingTimestamp && record.timestamp <= now)
      assertEquals(offset.toLong, record.offset)
      val keyAndValueIndex = startingKeyAndValueIndex + i
      assertEquals(s"key $keyAndValueIndex", new String(record.key))
      assertEquals(s"value $keyAndValueIndex", new String(record.message()))
      // this is true only because K and V are byte arrays
      assertEquals(s"key $keyAndValueIndex".length, record.key.length)
      assertEquals(s"value $keyAndValueIndex".length, record.message.length)
    }
  }

  protected def consumeRecords[K, V](consumer: ConsumerConnector,
                                     numRecords: Int,
                                     maxPollRecords: Int = Int.MaxValue): ArrayBuffer[MessageAndMetadata[Array[Byte], Array[Byte]]] = {
    val streams = consumer.createMessageStreams(Map(topic -> 1))
    val iterator = streams.get(topic).head.head.iterator()
    val messages = new ArrayBuffer[MessageAndMetadata[Array[Byte], Array[Byte]]]()
    val maxIters = numRecords * 300
    var iters = 0
    while (messages.size < numRecords) {
      try {
        messages += iterator.next()
      }
      catch {
        case e: ConsumerTimeoutException => // do nothing
      }
      if (iters > maxIters)
        throw new IllegalStateException("Failed to consume the expected records after " + iters + " iterations.")
      iters += 1
    }
    messages
  }
}