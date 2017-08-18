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

package integration.kafka.api

import java.io.File
import java.util.Properties

import org.apache.kafka.common.internals.Topic
import kafka.consumer.{Consumer, ConsumerConnector}
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.{After, Before}

import scala.collection.mutable.Buffer

/**
 * A helper class for writing integration tests that involve producers, consumers, and servers
 */
abstract class OldConsumerIntegrationTestHarness extends KafkaServerTestHarness {

  val producerCount: Int
  val consumerCount: Int
  val serverCount: Int
  lazy val producerConfig = new Properties
  lazy val consumerConfig = new Properties
  lazy val serverConfig = new Properties

  val consumers = Buffer[ConsumerConnector]()
  val producers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()

  override def generateConfigs() = {
    val cfgs = TestUtils.createBrokerConfigs(serverCount, zkConnect, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = clientSaslProperties)
    cfgs.foreach(_.putAll(serverConfig))
    cfgs.map(KafkaConfig.fromProps)
  }

  @Before
  override def setUp() {
    val producerSecurityProps = TestUtils.producerSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    val consumerSecurityProps = TestUtils.consumerSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    super.setUp()
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    producerConfig.putAll(producerSecurityProps)
    consumerConfig.putAll(consumerSecurityProps)
    consumerConfig.setProperty("zookeeper.connect", zkConnect)
    for (i <- 0 until producerCount)
      producers += createNewProducer
    for (i <- 0 until consumerCount) {
      consumers += createOldConsumer
    }

    // create the consumer offset topic
    TestUtils.createTopic(zkUtils, Topic.GROUP_METADATA_TOPIC_NAME,
      serverConfig.getProperty(KafkaConfig.OffsetsTopicPartitionsProp).toInt,
      serverConfig.getProperty(KafkaConfig.OffsetsTopicReplicationFactorProp).toInt,
      servers,
      servers.head.groupCoordinator.offsetsTopicConfigs)
  }

  def createNewProducer: KafkaProducer[Array[Byte], Array[Byte]] = {
    TestUtils.createNewProducer(brokerList,
      securityProtocol = this.securityProtocol,
      trustStoreFile = this.trustStoreFile,
      saslProperties = this.clientSaslProperties,
      props = Some(producerConfig))
  }

  def createOldConsumer: ConsumerConnector = {
    createOldConsumer(
      securityProtocol = this.securityProtocol,
      trustStoreFile = this.trustStoreFile,
      saslProperties = this.clientSaslProperties,
      props = Some(consumerConfig),
      brokerListOpt = if (securityProtocol == SecurityProtocol.SSL) Some(brokerList) else None
    )
  }

  @After
  override def tearDown() {
    producers.foreach(_.close())
    consumers.foreach(_.shutdown())
    super.tearDown()
  }

  /**
   * Create a new consumer with a few pre-configured properties.
   */
  def createOldConsumer(groupId: String = "group",
                        autoOffsetReset: String = "smallest",
                        partitionFetchSize: Long = 4096L,
                        partitionAssignmentStrategy: String = "range",
                        sessionTimeout: Int = 30000,
                        securityProtocol: SecurityProtocol,
                        trustStoreFile: Option[File] = None,
                        saslProperties: Option[Properties] = None,
                        props: Option[Properties] = None,
                        brokerListOpt: Option[String] = None) : ConsumerConnector = {
    import org.apache.kafka.clients.consumer.ConsumerConfig

    val consumerProps = props.getOrElse(new Properties())
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
    consumerProps.put("fetch.max.bytes", partitionFetchSize.toString)
    consumerProps.put("partition.assignment.strategy", partitionAssignmentStrategy)
    // TODO: determine retry backoff if necessary

    /*
     * It uses CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to determine whether
     * securityConfigs has been invoked already. For example, we need to
     * invoke it before this call in IntegrationTestHarness, otherwise the
     * SSL client auth fails.
     */
    if(!consumerProps.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG))
      consumerProps.putAll(TestUtils.consumerSecurityConfigs(securityProtocol, trustStoreFile, saslProperties))
    if (securityProtocol == SecurityProtocol.SSL)
      consumerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerListOpt.get)

    val config = new kafka.consumer.ConsumerConfig(consumerProps)
    Consumer.create(config)
  }

}