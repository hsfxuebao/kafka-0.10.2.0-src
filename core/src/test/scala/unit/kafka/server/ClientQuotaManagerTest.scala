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
package kafka.server

import java.util.Collections

import org.apache.kafka.common.metrics.{MetricConfig, Metrics, Quota}
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}
import kafka.admin.ConfigCommand

class ClientQuotaManagerTest {
  private val time = new MockTime

  private val config = ClientQuotaManagerConfig(quotaBytesPerSecondDefault = 500)

  var numCallbacks: Int = 0
  def callback(delayTimeMs: Int) {
    numCallbacks += 1
  }

  @Before
  def beforeMethod() {
    numCallbacks = 0
  }

  private def testQuotaParsing(config: ClientQuotaManagerConfig, client1: UserClient, client2: UserClient, randomClient: UserClient, defaultConfigClient: UserClient) {
    val clientMetrics = new ClientQuotaManager(config, newMetrics, QuotaType.Produce, time)

    try {
      // Case 1: Update the quota. Assert that the new quota value is returned
      clientMetrics.updateQuota(client1.configUser, client1.configClientId, Some(new Quota(2000, true)))
      clientMetrics.updateQuota(client2.configUser, client2.configClientId, Some(new Quota(4000, true)))

      assertEquals("Default producer quota should be " + config.quotaBytesPerSecondDefault, new Quota(config.quotaBytesPerSecondDefault, true), clientMetrics.quota(randomClient.user, randomClient.clientId))
      assertEquals("Should return the overridden value (2000)", new Quota(2000, true), clientMetrics.quota(client1.user, client1.clientId))
      assertEquals("Should return the overridden value (4000)", new Quota(4000, true), clientMetrics.quota(client2.user, client2.clientId))

      // p1 should be throttled using the overridden quota
      var throttleTimeMs = clientMetrics.recordAndMaybeThrottle(client1.user, client1.clientId, 2500 * config.numQuotaSamples, this.callback)
      assertTrue(s"throttleTimeMs should be > 0. was $throttleTimeMs", throttleTimeMs > 0)

      // Case 2: Change quota again. The quota should be updated within KafkaMetrics as well since the sensor was created.
      // p1 should not longer be throttled after the quota change
      clientMetrics.updateQuota(client1.configUser, client1.configClientId, Some(new Quota(3000, true)))
      assertEquals("Should return the newly overridden value (3000)", new Quota(3000, true), clientMetrics.quota(client1.user, client1.clientId))

      throttleTimeMs = clientMetrics.recordAndMaybeThrottle(client1.user, client1.clientId, 0, this.callback)
      assertEquals(s"throttleTimeMs should be 0. was $throttleTimeMs", 0, throttleTimeMs)

      // Case 3: Change quota back to default. Should be throttled again
      clientMetrics.updateQuota(client1.configUser, client1.configClientId, Some(new Quota(500, true)))
      assertEquals("Should return the default value (500)", new Quota(500, true), clientMetrics.quota(client1.user, client1.clientId))

      throttleTimeMs = clientMetrics.recordAndMaybeThrottle(client1.user, client1.clientId, 0, this.callback)
      assertTrue(s"throttleTimeMs should be > 0. was $throttleTimeMs", throttleTimeMs > 0)

      // Case 4: Set high default quota, remove p1 quota. p1 should no longer be throttled
      clientMetrics.updateQuota(client1.configUser, client1.configClientId, None)
      clientMetrics.updateQuota(defaultConfigClient.configUser, defaultConfigClient.configClientId, Some(new Quota(4000, true)))
      assertEquals("Should return the newly overridden value (4000)", new Quota(4000, true), clientMetrics.quota(client1.user, client1.clientId))

      throttleTimeMs = clientMetrics.recordAndMaybeThrottle(client1.user, client1.clientId, 1000 * config.numQuotaSamples, this.callback)
      assertEquals(s"throttleTimeMs should be 0. was $throttleTimeMs", 0, throttleTimeMs)

    } finally {
      clientMetrics.shutdown()
    }
  }

  /**
   * Tests parsing for <client-id> quotas.
   * Quota overrides persisted in Zookeeper in /config/clients/<client-id>, default persisted in /config/clients/<default>
   */
  @Test
  def testClientIdQuotaParsing() {
    val client1 = UserClient("ANONYMOUS", "p1", None, Some("p1"))
    val client2 = UserClient("ANONYMOUS", "p2", None, Some("p2"))
    val randomClient = UserClient("ANONYMOUS", "random-client-id", None, None)
    val defaultConfigClient = UserClient("", "", None, Some(ConfigEntityName.Default))
    testQuotaParsing(config, client1, client2, randomClient, defaultConfigClient)
  }

  /**
   * Tests parsing for <user> quotas.
   * Quota overrides persisted in Zookeeper in /config/users/<user>, default persisted in /config/users/<default>
   */
  @Test
  def testUserQuotaParsing() {
    val client1 = UserClient("User1", "p1", Some("User1"), None)
    val client2 = UserClient("User2", "p2", Some("User2"), None)
    val randomClient = UserClient("RandomUser", "random-client-id", None, None)
    val defaultConfigClient = UserClient("", "", Some(ConfigEntityName.Default), None)
    val config = ClientQuotaManagerConfig(quotaBytesPerSecondDefault = Long.MaxValue)
    testQuotaParsing(config, client1, client2, randomClient, defaultConfigClient)
  }

  /**
   * Tests parsing for <user, client-id> quotas.
   * Quotas persisted in Zookeeper in /config/users/<user>/clients/<client-id>, default in /config/users/<default>/clients/<default>
   */
  @Test
  def testUserClientIdQuotaParsing() {
    val client1 = UserClient("User1", "p1", Some("User1"), Some("p1"))
    val client2 = UserClient("User2", "p2", Some("User2"), Some("p2"))
    val randomClient = UserClient("RandomUser", "random-client-id", None, None)
    val defaultConfigClient = UserClient("", "", Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
    val config = ClientQuotaManagerConfig(quotaBytesPerSecondDefault = Long.MaxValue)
    testQuotaParsing(config, client1, client2, randomClient, defaultConfigClient)
  }

  /**
   * Tests parsing for <user> quotas when client-id default quota properties are set.
   */
  @Test
  def testUserQuotaParsingWithDefaultClientIdQuota() {
    val client1 = UserClient("User1", "p1", Some("User1"), None)
    val client2 = UserClient("User2", "p2", Some("User2"), None)
    val randomClient = UserClient("RandomUser", "random-client-id", None, None)
    val defaultConfigClient = UserClient("", "", Some(ConfigEntityName.Default), None)
    testQuotaParsing(config, client1, client2, randomClient, defaultConfigClient)
  }

  /**
   * Tests parsing for <user, client-id> quotas when client-id default quota properties are set.
   */
  @Test
  def testUserClientQuotaParsingIdWithDefaultClientIdQuota() {
    val client1 = UserClient("User1", "p1", Some("User1"), Some("p1"))
    val client2 = UserClient("User2", "p2", Some("User2"), Some("p2"))
    val randomClient = UserClient("RandomUser", "random-client-id", None, None)
    val defaultConfigClient = UserClient("", "", Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
    testQuotaParsing(config, client1, client2, randomClient, defaultConfigClient)
  }

  @Test
  def testQuotaConfigPrecedence() {
    val quotaManager = new ClientQuotaManager(ClientQuotaManagerConfig(quotaBytesPerSecondDefault=Long.MaxValue), newMetrics, QuotaType.Produce, time)

    def checkQuota(user: String, clientId: String, expectedBound: Int, value: Int, expectThrottle: Boolean) {
      assertEquals(new Quota(expectedBound, true), quotaManager.quota(user, clientId))
      val throttleTimeMs = quotaManager.recordAndMaybeThrottle(user, clientId, value * config.numQuotaSamples, this.callback)
      if (expectThrottle)
        assertTrue(s"throttleTimeMs should be > 0. was $throttleTimeMs", throttleTimeMs > 0)
      else
        assertEquals(s"throttleTimeMs should be 0. was $throttleTimeMs", 0, throttleTimeMs)
    }

    try {
      quotaManager.updateQuota(Some(ConfigEntityName.Default), None, Some(new Quota(1000, true)))
      quotaManager.updateQuota(None, Some(ConfigEntityName.Default), Some(new Quota(2000, true)))
      quotaManager.updateQuota(Some(ConfigEntityName.Default), Some(ConfigEntityName.Default), Some(new Quota(3000, true)))
      quotaManager.updateQuota(Some("userA"), None, Some(new Quota(4000, true)))
      quotaManager.updateQuota(Some("userA"), Some("client1"), Some(new Quota(5000, true)))
      quotaManager.updateQuota(Some("userB"), None, Some(new Quota(6000, true)))
      quotaManager.updateQuota(Some("userB"), Some("client1"), Some(new Quota(7000, true)))
      quotaManager.updateQuota(Some("userB"), Some(ConfigEntityName.Default), Some(new Quota(8000, true)))
      quotaManager.updateQuota(Some("userC"), None, Some(new Quota(10000, true)))
      quotaManager.updateQuota(None, Some("client1"), Some(new Quota(9000, true)))

      checkQuota("userA", "client1", 5000, 4500, false) // <user, client> quota takes precedence over <user>
      checkQuota("userA", "client2", 4000, 4500, true)  // <user> quota takes precedence over <client> and defaults
      checkQuota("userA", "client3", 4000, 0, true)     // <user> quota is shared across clients of user
      checkQuota("userA", "client1", 5000, 0, false)    // <user, client> is exclusive use, unaffected by other clients

      checkQuota("userB", "client1", 7000, 8000, true)
      checkQuota("userB", "client2", 8000, 7000, false) // Default per-client quota for exclusive use of <user, client>
      checkQuota("userB", "client3", 8000, 7000, false)

      checkQuota("userD", "client1", 3000, 3500, true)  // Default <user, client> quota
      checkQuota("userD", "client2", 3000, 2500, false)
      checkQuota("userE", "client1", 3000, 2500, false)

      // Remove default <user, client> quota config, revert to <user> default
      quotaManager.updateQuota(Some(ConfigEntityName.Default), Some(ConfigEntityName.Default), None)
      checkQuota("userD", "client1", 1000, 0, false)    // Metrics tags changed, restart counter
      checkQuota("userE", "client4", 1000, 1500, true)
      checkQuota("userF", "client4", 1000, 800, false)  // Default <user> quota shared across clients of user
      checkQuota("userF", "client5", 1000, 800, true)

      // Remove default <user> quota config, revert to <client-id> default
      quotaManager.updateQuota(Some(ConfigEntityName.Default), None, None)
      checkQuota("userF", "client4", 2000, 0, false)  // Default <client-id> quota shared across client-id of all users
      checkQuota("userF", "client5", 2000, 0, false)
      checkQuota("userF", "client5", 2000, 2500, true)
      checkQuota("userG", "client5", 2000, 0, true)

      // Update quotas
      quotaManager.updateQuota(Some("userA"), None, Some(new Quota(8000, true)))
      quotaManager.updateQuota(Some("userA"), Some("client1"), Some(new Quota(10000, true)))
      checkQuota("userA", "client2", 8000, 0, false)
      checkQuota("userA", "client2", 8000, 4500, true) // Throttled due to sum of new and earlier values
      checkQuota("userA", "client1", 10000, 0, false)
      checkQuota("userA", "client1", 10000, 6000, true)
      quotaManager.updateQuota(Some("userA"), Some("client1"), None)
      checkQuota("userA", "client6", 8000, 0, true)    // Throttled due to shared user quota
      quotaManager.updateQuota(Some("userA"), Some("client6"), Some(new Quota(11000, true)))
      checkQuota("userA", "client6", 11000, 8500, false)
      quotaManager.updateQuota(Some("userA"), Some(ConfigEntityName.Default), Some(new Quota(12000, true)))
      quotaManager.updateQuota(Some("userA"), Some("client6"), None)
      checkQuota("userA", "client6", 12000, 4000, true) // Throttled due to sum of new and earlier values

    } finally {
      quotaManager.shutdown()
    }
  }

  @Test
  def testQuotaViolation() {
    val metrics = newMetrics
    val clientMetrics = new ClientQuotaManager(config, metrics, QuotaType.Produce, time)
    val queueSizeMetric = metrics.metrics().get(metrics.metricName("queue-size", "Produce", ""))
    try {
      /* We have 10 second windows. Make sure that there is no quota violation
       * if we produce under the quota
       */
      for (_ <- 0 until 10) {
        clientMetrics.recordAndMaybeThrottle("ANONYMOUS", "unknown", 400, callback)
        time.sleep(1000)
      }
      assertEquals(10, numCallbacks)
      assertEquals(0, queueSizeMetric.value().toInt)

      // Create a spike.
      // 400*10 + 2000 + 300 = 6300/10.5 = 600 bytes per second.
      // (600 - quota)/quota*window-size = (600-500)/500*10.5 seconds = 2100
      // 10.5 seconds because the last window is half complete
      time.sleep(500)
      val sleepTime = clientMetrics.recordAndMaybeThrottle("ANONYMOUS", "unknown", 2300, callback)

      assertEquals("Should be throttled", 2100, sleepTime)
      assertEquals(1, queueSizeMetric.value().toInt)
      // After a request is delayed, the callback cannot be triggered immediately
      clientMetrics.throttledRequestReaper.doWork()
      assertEquals(10, numCallbacks)
      time.sleep(sleepTime)

      // Callback can only be triggered after the delay time passes
      clientMetrics.throttledRequestReaper.doWork()
      assertEquals(0, queueSizeMetric.value().toInt)
      assertEquals(11, numCallbacks)

      // Could continue to see delays until the bursty sample disappears
      for (_ <- 0 until 10) {
        clientMetrics.recordAndMaybeThrottle("ANONYMOUS", "unknown", 400, callback)
        time.sleep(1000)
      }

      assertEquals("Should be unthrottled since bursty sample has rolled over",
                   0, clientMetrics.recordAndMaybeThrottle("ANONYMOUS", "unknown", 0, callback))
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testExpireThrottleTimeSensor() {
    val metrics = newMetrics
    val clientMetrics = new ClientQuotaManager(config, metrics, QuotaType.Produce, time)
    try {
      clientMetrics.recordAndMaybeThrottle("ANONYMOUS", "client1", 100, callback)
      // remove the throttle time sensor
      metrics.removeSensor("ProduceThrottleTime-:client1")
      // should not throw an exception even if the throttle time sensor does not exist.
      val throttleTime = clientMetrics.recordAndMaybeThrottle("ANONYMOUS", "client1", 10000, callback)
      assertTrue("Should be throttled", throttleTime > 0)
      // the sensor should get recreated
      val throttleTimeSensor = metrics.getSensor("ProduceThrottleTime-:client1")
      assertTrue("Throttle time sensor should exist", throttleTimeSensor != null)
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testExpireQuotaSensors() {
    val metrics = newMetrics
    val clientMetrics = new ClientQuotaManager(config, metrics, QuotaType.Produce, time)
    try {
      clientMetrics.recordAndMaybeThrottle("ANONYMOUS", "client1", 100, callback)
      // remove all the sensors
      metrics.removeSensor("ProduceThrottleTime-:client1")
      metrics.removeSensor("Produce-ANONYMOUS:client1")
      // should not throw an exception
      val throttleTime = clientMetrics.recordAndMaybeThrottle("ANONYMOUS", "client1", 10000, callback)
      assertTrue("Should be throttled", throttleTime > 0)

      // all the sensors should get recreated
      val throttleTimeSensor = metrics.getSensor("ProduceThrottleTime-:client1")
      assertTrue("Throttle time sensor should exist", throttleTimeSensor != null)

      val byteRateSensor = metrics.getSensor("Produce-:client1")
      assertTrue("Byte rate sensor should exist", byteRateSensor != null)
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testQuotaUserSanitize() {
    val principal = "CN=Some characters !@#$%&*()_-+=';:,/~"
    val sanitizedPrincipal = QuotaId.sanitize(principal)
    // Apart from % used in percent-encoding all characters of sanitized principal must be characters allowed in client-id
    ConfigCommand.validateChars("sanitized-principal", sanitizedPrincipal.replace('%', '_'))
    assertEquals(principal, QuotaId.desanitize(sanitizedPrincipal))
  }

  def newMetrics: Metrics = {
    new Metrics(new MetricConfig(), Collections.emptyList(), time)
  }

  private case class UserClient(val user: String, val clientId: String, val configUser: Option[String] = None, val configClientId: Option[String] = None)
}
