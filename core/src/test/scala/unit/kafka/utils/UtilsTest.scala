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

import java.util.{Arrays, UUID}
import java.util.concurrent.locks.ReentrantLock
import java.nio.ByteBuffer
import java.util.regex.Pattern

import org.apache.log4j.Logger
import org.scalatest.junit.JUnitSuite
import org.junit.Assert._
import kafka.common.KafkaException
import kafka.utils.CoreUtils.inLock
import org.junit.Test
import org.apache.kafka.common.utils.Utils

class UtilsTest extends JUnitSuite {

  private val logger = Logger.getLogger(classOf[UtilsTest])
  val clusterIdPattern = Pattern.compile("[a-zA-Z0-9_\\-]+")

  @Test
  def testSwallow() {
    CoreUtils.swallow(logger.info, throw new KafkaException("test"))
  }

  @Test
  def testCircularIterator() {
    val l = List(1, 2)
    val itl = CoreUtils.circularIterator(l)
    assertEquals(1, itl.next())
    assertEquals(2, itl.next())
    assertEquals(1, itl.next())
    assertEquals(2, itl.next())
    assertFalse(itl.hasDefiniteSize)

    val s = Set(1, 2)
    val its = CoreUtils.circularIterator(s)
    assertEquals(1, its.next())
    assertEquals(2, its.next())
    assertEquals(1, its.next())
    assertEquals(2, its.next())
    assertEquals(1, its.next())
  }

  @Test
  def testReadBytes() {
    for(testCase <- List("", "a", "abcd")) {
      val bytes = testCase.getBytes
      assertTrue(Arrays.equals(bytes, Utils.readBytes(ByteBuffer.wrap(bytes))))
    }
  }

  @Test
  def testAbs() {
    assertEquals(0, Utils.abs(Integer.MIN_VALUE))
    assertEquals(1, Utils.abs(-1))
    assertEquals(0, Utils.abs(0))
    assertEquals(1, Utils.abs(1))
    assertEquals(Integer.MAX_VALUE, Utils.abs(Integer.MAX_VALUE))
  }

  @Test
  def testReplaceSuffix() {
    assertEquals("blah.foo.text", CoreUtils.replaceSuffix("blah.foo.txt", ".txt", ".text"))
    assertEquals("blah.foo", CoreUtils.replaceSuffix("blah.foo.txt", ".txt", ""))
    assertEquals("txt.txt", CoreUtils.replaceSuffix("txt.txt.txt", ".txt", ""))
    assertEquals("foo.txt", CoreUtils.replaceSuffix("foo", "", ".txt"))
  }

  @Test
  def testReadInt() {
    val values = Array(0, 1, -1, Byte.MaxValue, Short.MaxValue, 2 * Short.MaxValue, Int.MaxValue/2, Int.MinValue/2, Int.MaxValue, Int.MinValue, Int.MaxValue)
    val buffer = ByteBuffer.allocate(4 * values.size)
    for(i <- 0 until values.length) {
      buffer.putInt(i*4, values(i))
      assertEquals("Written value should match read value.", values(i), CoreUtils.readInt(buffer.array, i*4))
    }
  }

  @Test
  def testCsvList() {
    val emptyString:String = ""
    val nullString:String = null
    val emptyList = CoreUtils.parseCsvList(emptyString)
    val emptyListFromNullString = CoreUtils.parseCsvList(nullString)
    val emptyStringList = Seq.empty[String]
    assertTrue(emptyList!=null)
    assertTrue(emptyListFromNullString!=null)
    assertTrue(emptyStringList.equals(emptyListFromNullString))
    assertTrue(emptyStringList.equals(emptyList))
  }

  @Test
  def testCsvMap() {
    val emptyString: String = ""
    val emptyMap = CoreUtils.parseCsvMap(emptyString)
    val emptyStringMap = Map.empty[String, String]
    assertTrue(emptyMap != null)
    assertTrue(emptyStringMap.equals(emptyStringMap))

    val kvPairsIpV6: String = "a:b:c:v,a:b:c:v"
    val ipv6Map = CoreUtils.parseCsvMap(kvPairsIpV6)
    for (m <- ipv6Map) {
      assertTrue(m._1.equals("a:b:c"))
      assertTrue(m._2.equals("v"))
    }

    val singleEntry:String = "key:value"
    val singleMap = CoreUtils.parseCsvMap(singleEntry)
    val value = singleMap.getOrElse("key", 0)
    assertTrue(value.equals("value"))

    val kvPairsIpV4: String = "192.168.2.1/30:allow, 192.168.2.1/30:allow"
    val ipv4Map = CoreUtils.parseCsvMap(kvPairsIpV4)
    for (m <- ipv4Map) {
      assertTrue(m._1.equals("192.168.2.1/30"))
      assertTrue(m._2.equals("allow"))
    }

    val kvPairsSpaces: String = "key:value      , key:   value"
    val spaceMap = CoreUtils.parseCsvMap(kvPairsSpaces)
    for (m <- spaceMap) {
      assertTrue(m._1.equals("key"))
      assertTrue(m._2.equals("value"))
    }
  }

  @Test
  def testInLock() {
    val lock = new ReentrantLock()
    val result = inLock(lock) {
      assertTrue("Should be in lock", lock.isHeldByCurrentThread)
      1 + 1
    }
    assertEquals(2, result)
    assertFalse("Should be unlocked", lock.isLocked)
  }

  @Test
  def testUrlSafeBase64EncodeUUID() {

    // Test a UUID that has no + or / characters in base64 encoding [a149b4a3-06e1-4b49-a8cb-8a9c4a59fa46 ->(base64)-> oUm0owbhS0moy4qcSln6Rg==]
    val clusterId1 = CoreUtils.urlSafeBase64EncodeNoPadding(CoreUtils.getBytesFromUuid(UUID.fromString("a149b4a3-06e1-4b49-a8cb-8a9c4a59fa46")))
    assertEquals(clusterId1, "oUm0owbhS0moy4qcSln6Rg")
    assertEquals(clusterId1.length, 22)
    assertTrue(clusterIdPattern.matcher(clusterId1).matches())

    // Test a UUID that has + or / characters in base64 encoding [d418ec02-277e-4853-81e6-afe30259daec ->(base64)-> 1BjsAid+SFOB5q/jAlna7A==]
    val clusterId2 = CoreUtils.urlSafeBase64EncodeNoPadding(CoreUtils.getBytesFromUuid(UUID.fromString("d418ec02-277e-4853-81e6-afe30259daec")))
    assertEquals(clusterId2, "1BjsAid-SFOB5q_jAlna7A")
    assertEquals(clusterId2.length, 22)
    assertTrue(clusterIdPattern.matcher(clusterId2).matches())
  }

  @Test
  def testGenerateUuidAsBase64() {
    val clusterId = CoreUtils.generateUuidAsBase64()
    assertEquals(clusterId.length, 22)
    assertTrue(clusterIdPattern.matcher(clusterId).matches())
  }
}
