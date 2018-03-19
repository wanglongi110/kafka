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

import java.io.File
import java.net.InetAddress
import java.util.Locale
import java.util.concurrent.TimeUnit

import kafka.api.ApiVersion
import kafka.cluster.EndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.network.RequestChannel
import kafka.utils._
import org.I0Itec.zkclient.IZkStateListener
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.utils.{PoisonPill, Time}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.zookeeper.Watcher.Event.KeeperState

/**
 * This class registers the broker in zookeeper to allow
 * other brokers and consumers to detect failures. It uses an ephemeral znode with the path:
 *   /brokers/ids/[0...N] --> advertisedHost:advertisedPort
 *
 * Right now our definition of health is fairly naive. If we register in zk we are healthy, otherwise
 * we are dead.
 */
class KafkaHealthcheck(brokerId: Int,
                       advertisedEndpoints: Seq[EndPoint],
                       zkUtils: ZkUtils,
                       rack: Option[String],
                       interBrokerProtocolVersion: ApiVersion,
                       requestChannel: RequestChannel,
                       requestProcessingMaxTimeMs: Long,
                       time: Time,
                       heapDumpFolder: File,
                       heapDumpTimeout: Long) extends Logging {

  private[server] val sessionExpireListener = new SessionExpireListener
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "kafka-healthcheck-scheduler-")

  def startup() {
    zkUtils.subscribeStateChanges(sessionExpireListener)
    register()
    scheduler.startup()
    scheduler.schedule(name = "halt-broker-if-not-healthy",
                       fun = haltIfNotHealthy,
                       period = 10000,
                       unit = TimeUnit.MILLISECONDS)
  }

  def shutdown() = {
    scheduler.shutdown()
    zkUtils.zkClient.unsubscribeStateChanges(sessionExpireListener)
  }

  private def haltIfNotHealthy() {
    // This relies on io-thread to receive request from RequestChannel with 300 ms timeout, so that lastDequeueTimeMs
    // will keep increasing even if there is no incoming request
    if (time.milliseconds - requestChannel.lastDequeueTimeMs > requestProcessingMaxTimeMs) {
      fatal(s"It has been more than $requestProcessingMaxTimeMs ms since the last time any io-thread reads from RequestChannel. Shutdown broker now.")
      PoisonPill.die(heapDumpFolder, heapDumpTimeout)
    }
  }

  /**
   * Register this broker as "alive" in zookeeper
   */
  def register() {
    val jmxPort = System.getProperty("com.sun.management.jmxremote.port", "-1").toInt
    var updatedEndpoints = advertisedEndpoints.map(endpoint =>
      if (endpoint.host == null || endpoint.host.trim.isEmpty)
        endpoint.copy(host = InetAddress.getLocalHost.getCanonicalHostName)
      else
        endpoint
    )

    // the default host and port are here for compatibility with older clients that only support PLAINTEXT
    // we choose the first plaintext port, if there is one
    // or we register an empty endpoint, which means that older clients will not be able to connect
    val plaintextEndpoint = updatedEndpoints.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).getOrElse(
      new EndPoint(null, -1, null, null))

    // HOTFIX for LIKAFKA-15620. We add a dummy plaintext endpoint for KAC cluster.
    // This HOTFIX can be removed after we deprecate brooklin-li-common_7.0.10 and earlier versions.
    if (plaintextEndpoint.host == null) {
      updatedEndpoints.find(endpoint => endpoint.listenerName.value().equals("SSL")) match {
        case Some(sslEndPoint) =>
          updatedEndpoints = updatedEndpoints ++ List(new EndPoint(sslEndPoint.host, 0, new ListenerName("PLAINTEXT"), SecurityProtocol.PLAINTEXT))
        case None =>
      }
    }

    zkUtils.registerBrokerInZk(brokerId, plaintextEndpoint.host, plaintextEndpoint.port, updatedEndpoints, jmxPort, rack,
      interBrokerProtocolVersion)
  }

  /**
   *  When we get a SessionExpired event, it means that we have lost all ephemeral nodes and ZKClient has re-established
   *  a connection for us. We need to re-register this broker in the broker registry. We rely on `handleStateChanged`
   *  to record ZooKeeper connection state metrics.
   */
  class SessionExpireListener extends IZkStateListener with KafkaMetricsGroup {

    private[server] val stateToMeterMap = {
      import KeeperState._
      val stateToEventTypeMap = Map(
        Disconnected -> "Disconnects",
        SyncConnected -> "SyncConnects",
        AuthFailed -> "AuthFailures",
        ConnectedReadOnly -> "ReadOnlyConnects",
        SaslAuthenticated -> "SaslAuthentications",
        Expired -> "Expires"
      )
      stateToEventTypeMap.map { case (state, eventType) =>
        state -> newMeter(s"ZooKeeper${eventType}PerSec", eventType.toLowerCase(Locale.ROOT), TimeUnit.SECONDS)
      }
    }

    @throws[Exception]
    override def handleStateChanged(state: KeeperState) {
      stateToMeterMap.get(state).foreach(_.mark())
    }

    @throws[Exception]
    override def handleNewSession() {
      info("re-registering broker info in ZK for broker " + brokerId)
      register()
      info("done re-registering broker")
      info("Subscribing to %s path to watch for new topics".format(ZkUtils.BrokerTopicsPath))
    }

    override def handleSessionEstablishmentError(error: Throwable) {
      fatal("Could not establish session with zookeeper", error)
    }

  }

}
