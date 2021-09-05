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

import kafka.utils.CoreUtils._
import kafka.utils.{Json, Logging, ZKCheckedEphemeral}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.IZkDataListener
import kafka.controller.ControllerContext
import kafka.controller.KafkaController
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
 */
class ZookeeperLeaderElector(controllerContext: ControllerContext,
                             electionPath: String,
                             onBecomingLeader: () => Unit,
                             onResigningAsLeader: () => Unit,
                             brokerId: Int,
                             time: Time)
  extends LeaderElector with Logging {
  var leaderId = -1
  // create the election path in ZK, if one does not exist
  val index = electionPath.lastIndexOf("/")
  if (index > 0)
    controllerContext.zkUtils.makeSurePersistentPathExists(electionPath.substring(0, index))
  val leaderChangeListener = new LeaderChangeListener

  def startup {
    inLock(controllerContext.controllerLock) {
      // todo 对zk 上面的/controller某个目录 注册监听器
      controllerContext.zkUtils.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)
      // todo  选举
      elect
    }
  }

  // /controller 写数据（broker id号）
  def getControllerID(): Int = {
    // 从/controller目录下面去获取数据
    controllerContext.zkUtils.readDataMaybeNull(electionPath)._1 match {
        // 获取到了数据 返回一个id 这个id号就是某个broker id号  也就是这个broler id 就是controller
       case Some(controller) => KafkaController.parseControllerId(controller)
         // 如果获取不到就返回-1
       case None => -1
    }
  }

  def elect: Boolean = {
    val timestamp = time.milliseconds.toString
    /**
     * 构建数据信息 比如version ,brokerId 时间戳等
     */
    val electString = Json.encode(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp))

    /***
     * 去获取controller的id 号
     * 我们使用场景驱动的方式 此时应该就是我们的第一台服务器
     * 第一次启动 那么肯定没有controller的，所有在这获取不到（返回-1）
     */
    leaderId = getControllerID
    /* 
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition, 
     * it's possible that the controller has already been elected when we get here. This check will prevent the following 
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    if(leaderId != -1) {
       debug("Broker %d has been elected as leader, so stopping the election process.".format(leaderId))
       // 如果代码执行到这，说明之前已经完成选举了
       return amILeader
    }

    try {
      // 创建一个临时目录（/controller）
      // 然后往目录里面写上自己的信息
      val zkCheckedEphemeral = new ZKCheckedEphemeral(electionPath,
                                                      electString,
                                                      controllerContext.zkUtils.zkConnection.getZookeeper,
                                                      JaasUtils.isZkSecurityEnabled())
      // 创建目录
      zkCheckedEphemeral.create()
      info(brokerId + " successfully elected as leader")
      // 也就是当前服务器就是controller服务器了
      leaderId = brokerId
      /** 如果创建完了 自己就成为leader 也就是controller
       * 这是一个函数
       * 当一个controller被选举出来以后，就会执行这个函数
      */
      onBecomingLeader()
    } catch {
      case _: ZkNodeExistsException =>
        // If someone else has written the path, then
        leaderId = getControllerID 

        if (leaderId != -1)
          debug("Broker %d was elected as leader instead of broker %d".format(leaderId, brokerId))
        else
          warn("A leader has been elected but just resigned, this will result in another round of election")

      case e2: Throwable =>
        error("Error while electing or becoming leader on broker %d".format(brokerId), e2)
        resign()
    }
    amILeader
  }

  def close = {
    leaderId = -1
  }

  def amILeader : Boolean = leaderId == brokerId

  def resign() = {
    leaderId = -1
    controllerContext.zkUtils.deletePath(electionPath)
  }

  /**
   * We do not have session expiration listen in the ZkElection, but assuming the caller who uses this module will
   * have its own session expiration listener and handler
   */
  class LeaderChangeListener extends IZkDataListener with Logging {
    /**
     * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
     * @throws Exception On any error.
     */
    @throws[Exception]
    def handleDataChange(dataPath: String, data: Object) {
      val shouldResign = inLock(controllerContext.controllerLock) {
        val amILeaderBeforeDataChange = amILeader
        leaderId = KafkaController.parseControllerId(data.toString)
        info("New leader is %d".format(leaderId))
        // The old leader needs to resign leadership if it is no longer the leader
        amILeaderBeforeDataChange && !amILeader
      }

      if (shouldResign)
        onResigningAsLeader()
    }

    /**
     * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
     * @throws Exception
     *             On any error.
     */
    @throws[Exception]
    def handleDataDeleted(dataPath: String) { 
      val shouldResign = inLock(controllerContext.controllerLock) {
        debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
          .format(brokerId, dataPath))
        amILeader
      }

      if (shouldResign)
        onResigningAsLeader()

      inLock(controllerContext.controllerLock) {
        elect
      }
    }
  }
}
