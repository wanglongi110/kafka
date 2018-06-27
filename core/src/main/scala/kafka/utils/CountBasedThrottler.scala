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

import java.util.concurrent.TimeUnit

import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.utils.Time

import scala.math._

/**
  * CountBasedThrottler is an implementation of Throttler and will check the rate and throttle only when the number of
  * observed items goes beyond a threshold.
  *
  * @param desiredRatePerSec: The rate we want to hit in units/sec
  * @param checkIntervalNum: The interval at which to check our rate
  * @param throttleDown: Does throttling increase or decrease our rate?
  * @param time: The time implementation to use
  * @param needThrottle: a boolean function to decide whether to throttle the rate in runtime
  * @param tags: tags for the rate metrics
  */
@threadsafe
class CountBasedThrottler(desiredRatePerSec: Double,
                          checkIntervalNum: Double = 10.0,
                          throttleDown: Boolean = true,
                          metricName: String = "throttler",
                          units: String = "entries",
                          time: Time = Time.SYSTEM,
                          needThrottle: () => Boolean = () => true,
                          tags: Map[String, String] = Map.empty) extends Logging with KafkaMetricsGroup {

  private val lock = new Object
  private val meter = newMeter(metricName, units, TimeUnit.SECONDS, tags)
  private var periodStartNs: Long = time.nanoseconds
  private var observedSoFar: Double = 0.0
  private val msPerSec = TimeUnit.SECONDS.toMillis(1)
  private val nsPerSec = TimeUnit.SECONDS.toNanos(1)

  def maybeThrottle(observed: Double) {
    lock synchronized {
      observedSoFar += observed
      // if we have completed an interval, maybe we should take a little nap
      if (observedSoFar > checkIntervalNum) {
        val elapsedNs = time.nanoseconds() - periodStartNs
        val rateInSecs = (observedSoFar * nsPerSec) / elapsedNs
        val needAdjustment = !(throttleDown ^ (rateInSecs > desiredRatePerSec)) && needThrottle()
        if (needAdjustment) {
          // solve for the amount of time to sleep to make us hit the desired rate
          val desiredRateMs = desiredRatePerSec / msPerSec.toDouble
          val elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNs)
          val sleepTime = round(observedSoFar / desiredRateMs - elapsedMs)
          if (sleepTime > 0) {
            trace("Natural rate is %f per second but desired rate is %f, sleeping for %d ms to compensate.".format(rateInSecs, desiredRatePerSec, sleepTime))
            time.sleep(sleepTime)
          }
        }
        periodStartNs = time.nanoseconds()
        observedSoFar = 0
      }
    }
    meter.mark(observed.toLong)
  }
}
