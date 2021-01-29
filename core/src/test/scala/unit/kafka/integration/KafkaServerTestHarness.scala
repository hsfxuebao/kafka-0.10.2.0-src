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

package kafka.integration

import java.io.File
import java.util.Arrays

import kafka.common.KafkaException
import kafka.server._
import kafka.utils.{CoreUtils, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.{After, Before}

import scala.collection.mutable.Buffer
import java.util.Properties

import org.apache.kafka.common.network.ListenerName

/**
 * A test harness that brings up some number of broker nodes
 */
trait KafkaServerTestHarness extends ZooKeeperTestHarness {
  var instanceConfigs: Seq[KafkaConfig] = null
  var servers: Buffer[KafkaServer] = null
  var brokerList: String = null
  var alive: Array[Boolean] = null
  val kafkaPrincipalType = KafkaPrincipal.USER_TYPE

  /**
   * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
   * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
   */
  def generateConfigs(): Seq[KafkaConfig]

  /**
   * Override this in case ACLs or security credentials must be set before `servers` are started.
   *
   * This is required in some cases because of the topic creation in the setup of `IntegrationTestHarness`. If the ACLs
   * are only set later, tests may fail. The failure could manifest itself as a cluster action
   * authorization exception when processing an update metadata request (controller -> broker) or in more obscure
   * ways (e.g. __consumer_offsets topic replication fails because the metadata cache has no brokers as a previous
   * update metadata request failed due to an authorization exception).
   *
   * The default implementation of this method is a no-op.
   */
  def configureSecurityBeforeServersStart() {}

  def configs: Seq[KafkaConfig] = {
    if (instanceConfigs == null)
      instanceConfigs = generateConfigs()
    instanceConfigs
  }

  def serverForId(id: Int): Option[KafkaServer] = servers.find(s => s.config.brokerId == id)

  def boundPort(server: KafkaServer): Int = server.boundPort(listenerName)

  protected def securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT
  protected def listenerName: ListenerName = ListenerName.forSecurityProtocol(securityProtocol)
  protected def trustStoreFile: Option[File] = None
  protected def serverSaslProperties: Option[Properties] = None
  protected def clientSaslProperties: Option[Properties] = None

  @Before
  override def setUp() {
    super.setUp

    if (configs.isEmpty)
      throw new KafkaException("Must supply at least one server config.")

    // default implementation is a no-op, it is overridden by subclasses if required
    configureSecurityBeforeServersStart()

    servers = configs.map(TestUtils.createServer(_)).toBuffer
    brokerList = TestUtils.getBrokerListStrFromServers(servers, securityProtocol)
    alive = new Array[Boolean](servers.length)
    Arrays.fill(alive, true)
  }

  @After
  override def tearDown() {
    if (servers != null) {
      servers.foreach(_.shutdown())
      servers.foreach(server => CoreUtils.delete(server.config.logDirs))
    }
    super.tearDown
  }
  
  /**
   * Pick a broker at random and kill it if it isn't already dead
   * Return the id of the broker killed
   */
  def killRandomBroker(): Int = {
    val index = TestUtils.random.nextInt(servers.length)
    killBroker(index)
    index
  }

  def killBroker(index: Int) {
    if(alive(index)) {
      servers(index).shutdown()
      servers(index).awaitShutdown()
      alive(index) = false
    }
  }
  
  /**
   * Restart any dead brokers
   */
  def restartDeadBrokers() {
    for(i <- servers.indices if !alive(i)) {
      servers(i).startup()
      alive(i) = true
    }
  }
}
