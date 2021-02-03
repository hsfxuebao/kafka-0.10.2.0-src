/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The range assignor works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumers in lexicographic order. We then divide the number of partitions by the total number of
 * consumers to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition.
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
 *
 * The assignment will be:
 * C0: [t0p0, t0p1, t1p0, t1p1]
 * C1: [t0p2, t1p2]
 */
public class RangeAssignor extends AbstractPartitionAssignor {

    @Override
    public String name() {
        return "range";
    }

    // Map<String MemberID, List 主题集合> -> Map<String 主题名称, List MemberID集合>
    private Map<String, List<String>> consumersPerTopic(Map<String, List<String>> consumerMetadata) {
        Map<String, List<String>> res = new HashMap<>();
        for (Map.Entry<String, List<String>> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            for (String topic : subscriptionEntry.getValue())
                put(res, topic, consumerId);
        }
        return res;
    }

    /**
     * 分配分区
     * @param partitionsPerTopic 集群元数据中保存的信息，键为Topic名称，值为该Topic拥有的分区数；
     * @param subscriptions Group里每个Member订阅的主题，键为Member ID，值为订阅的主题集合
     * @return 分区分配信息，键为MemberID, 值为分配给该MemberID的分区TopicPartition对象的集合
     */
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, List<String>> subscriptions) {
        /**
         * 转换Map结构：Map<String MemberID, List 主题集合> -> Map<String 主题名称, List MemberID集合>
         * 从<MemberID, 该Member订阅的主题名称集合>  转换为<主题名称, 订阅该主题的MemberID集合>
         */
        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        // Map<String MemberID, List 分配到的的主题分区集合>
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        // 遍历subscriptions中所有的MemberID，为每个Member创建一个ArrayList
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<TopicPartition>());

        // 遍历，每个键值对结构是：<主题名称, 订阅该主题的MemberID集合>
        for (Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {
            // 主题名称
            String topic = topicEntry.getKey();
            // 订阅该主题的MemberID集合
            List<String> consumersForTopic = topicEntry.getValue();
            // 获取主题的分区数
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null)
                continue;

            // 对订阅该主题的所有Member进行排序
            Collections.sort(consumersForTopic);

            // 每个Member可以分到的分区数
            int numPartitionsPerConsumer = numPartitionsForTopic / consumersForTopic.size();
            // 平均分配后额外多出的分区数
            int consumersWithExtraPartition = numPartitionsForTopic % consumersForTopic.size();

            /**
             * 根据主题与其分区数得到TopicPartition集合，实现比较简单，例如主题名称为test，分区数为3，最终会得到：
             * TopicPartition("test", 0), TopicPartition("test", 1), TopicPartition("test", 2) 三个对象组成的集合
             */
            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);
            /**
             * 根据Member的数量遍历相应的次数，分配当前topic的分区。假设test主题有7个分区，分配给2个Member，则有：
             * numPartitionsPerConsumer = 7 / 2 = 3
             * consumersWithExtraPartition = 7 % 2 = 1
             * 1. i = 0，分配第1个Member的分区
             *      start = 3 * 0 + Math.min(0, 1) = 0
             *      length = 3 + ((0 + 1) > 1 ? 0 : 1) = 4
             *   因此第1个Member得到： 0, 1, 2, 3 四个分区
             * 2. i = 1, 分配第2个Member的分区
             *      start = 3 * 1 + Math.min(1, 1) = 4
             *      length = 3 + ((1 + 1) > 1 ? 0 : 1) = 3
             *   因此第2个Member得到： 4, 5, 6 三个分区
             */
            for (int i = 0, n = consumersForTopic.size(); i < n; i++) {
                // 起始索引
                int start = numPartitionsPerConsumer * i + Math.min(i, consumersWithExtraPartition);
                // 长度
                int length = numPartitionsPerConsumer + (i + 1 > consumersWithExtraPartition ? 0 : 1);
                // 取partitions中范围为[start, start + length)子列表作为该MemberID分配到的分区信息
                assignment.get(consumersForTopic.get(i)).addAll(partitions.subList(start, start + length));
            }
        }

        /**
         * 订阅多个主题的情况：假设test1主题有7个分区，test2主题有3个分区，分配给2个Member，则有：
         * 1. 对于test1主题
         *      numPartitionsPerConsumer = 7 / 2 = 3
         *      consumersWithExtraPartition = 7 % 2 = 1
         *      1. i = 0，分配第1个Member的分区
         *              start = 3 * 0 + Math.min(0, 1) = 0
         *              length = 3 + ((0 + 1) > 1 ? 0 : 1) = 4
         *          因此第1个Member得到： 0, 1, 2, 3 四个分区
         *      2. i = 1, 分配第2个Member的分区
         *              start = 3 * 1 + Math.min(1, 1) = 4
         *              length = 3 + ((1 + 1) > 1 ? 0 : 1) = 3
         *          因此第2个Member得到： 4, 5, 6 三个分区
         * 2. 对于test2主题
         *      numPartitionsPerConsumer = 3 / 2 = 1
         *      consumersWithExtraPartition = 3 % 2 = 1
         *      1. i = 0，分配第1个Member的分区
         *              start = 1 * 0 + Math.min(0, 1) = 0
         *              length = 1 + ((0 + 1) > 1 ? 0 : 1) = 2
         *          因此第1个Member得到： 0, 1 两个分区
         *      2. i = 1, 分配第2个Member的分区
         *              start = 1 * 1 + Math.min(1, 1) = 2
         *              length = 1 + ((1 + 1) > 1 ? 0 : 1) = 1
         *          因此第2个Member得到： 3 一个分区
         * 最终结果：
         *      第一个Member：test1-[0, 1, 2, 3], test2-[0, 1]
         *      第二个Member：test1-[4, 5, 6], test2-[2]
         */
        return assignment;
    }

}
