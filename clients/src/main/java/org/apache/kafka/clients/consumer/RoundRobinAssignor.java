/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.CircularIterator;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * The round robin assignor lays out all the available partitions and all the available consumers. It
 * then proceeds to do a round robin assignment from partition to consumer. If the subscriptions of all consumer
 * instances are identical, then the partitions will be uniformly distributed. (i.e., the partition ownership counts
 * will be within a delta of exactly one across all consumers.)
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
 *
 * The assignment will be:
 * C0: [t0p0, t0p2, t1p1]
 * C1: [t0p1, t1p0, t1p2]
 *
 * When subscriptions differ across consumer instances, the assignment process still considers each
 * consumer instance in round robin fashion but skips over an instance if it is not subscribed to
 * the topic. Unlike the case when subscriptions are identical, this can result in imbalanced
 * assignments. For example, we have three consumers C0, C1, C2, and three topics t0, t1, t2,
 * with 1, 2, and 3 partitions, respectively. Therefore, the partitions are t0p0, t1p0, t1p1, t2p0,
 * t2p1, t2p2. C0 is subscribed to t0; C1 is subscribed to t0, t1; and C2 is subscribed to t0, t1, t2.
 *
 * Tha assignment will be:
 * C0: [t0p0]
 * C1: [t1p0]
 * C2: [t1p1, t2p0, t2p1, t2p2]
 */
public class RoundRobinAssignor extends AbstractPartitionAssignor {

    /**
     * 分配分区
     * @param partitionsPerTopic 集群元数据中保存的信息，键为Topic名称，值为该Topic拥有的分区数；
     * @param subscriptions Group里每个Member订阅的主题，键为Member ID，值为订阅的主题集合
     * @return 分区分配信息，键为MemberID, 值为分配给该MemberID的分区TopicPartition对象的集合
     */
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, List<String>> subscriptions) {
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        // 遍历subscriptions中所有的MemberID，为每个Member创建一个ArrayList
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<TopicPartition>());

        // 根据所有MemberID创建一个无限循环的迭代器，会对所有MemberID构成的列表进行从头至尾循环迭代
        CircularIterator<String> assigner = new CircularIterator<>(Utils.sorted(subscriptions.keySet()));
        /**
         * 订阅多个主题的情况：假设test1主题有7个分区，test2主题有3个分区，分配给2个Member，则有：
         * 1. 先对主题的分区进行排序，得到：
         *      TP(test1, 0), TP(test1, 1), TP(test1, 2), TP(test1, 3), TP(test1, 4), TP(test1, 5), TP(test1, 6),
         *      TP(test2, 0), TP(test2, 1), TP(test2, 2)
         * 2. 然后对Member进行排序，得到：Member-1，Member-2
         * 3. 进行轮询分配：
         *      TP(test1, 0)：Member-1
         *      TP(test1, 1)：Member-2
         *      TP(test1, 2)：Member-1
         *      TP(test1, 3)：Member-2
         *      TP(test1, 4)：Member-1
         *      TP(test1, 5)：Member-2
         *      TP(test1, 6)：Member-1
         *      TP(test2, 0)：Member-2
         *      TP(test2, 1)：Member-1
         *      TP(test2, 2)：Member-2
         * 4. 最终结果：
         *      第一个Member：test1-[0, 2, 4, 6], test2-[1]
         *      第二个Member：test1-[1, 3, 5], test2-[0, 2]
         */
        for (TopicPartition partition : allPartitionsSorted(partitionsPerTopic, subscriptions)) {
            // 获取主题
            final String topic = partition.topic();
            // 获取一个MemberID，查看该Member是否订阅了topic主题，如果没有就后移
            while (!subscriptions.get(assigner.peek()).contains(topic))
                assigner.next();
            // 直到循环迭代遇到订阅了topic的Member，将该TopicPartition分配给这个Member
            assignment.get(assigner.next()).add(partition);
        }
        return assignment;
    }


    /**
     * 对主题及其分区进行排序
     * @param partitionsPerTopic 集群元数据中保存的信息，键为Topic名称，值为该Topic拥有的分区数；
     * @param subscriptions Group里每个Member订阅的主题，键为Member ID，值为订阅的主题集合
     * @return 最终会得到所有订阅的主题及分区的集合，里面都是TopicPartition，且严格按照Topic名称、分区编号进行排序
     */
    public List<TopicPartition> allPartitionsSorted(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, List<String>> subscriptions) {
        // 使用TreeSet对Member订阅的所有主题集合进行排序
        SortedSet<String> topics = new TreeSet<>();
        for (List<String> subscription : subscriptions.values())
            topics.addAll(subscription);

        // 定义集合
        List<TopicPartition> allPartitions = new ArrayList<>();
        // 遍历排序后的主题集合
        for (String topic : topics) {
            // 获取主题的分区数
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic != null)
                // 向allPartitions添加主题的所有分区对应的TopicPartition对象
                allPartitions.addAll(AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic));
        }
        /**
         * 最终allPartitions集合中的元素是TopicPartition，且严格按照Topic名称、分区编号进行排序
         * 假设有test1，test2两个主题，test1有3个分区，test2有2个分区，得到的结果为：
         * TP(test1, 0), TP(test1, 1), TP(test1, 2), TP(test2, 0), TP(test1, 1)
         */
        return allPartitions;
    }

    @Override
    public String name() {
        return "roundrobin";
    }

}
