/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;


import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StreamsMetricsImplTest {

    @Test(expected = NullPointerException.class)
    public void testNullMetrics() throws Exception {
        String groupName = "doesNotMatter";
        Map<String, String> tags = new HashMap<>();
        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(null, groupName, tags);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveNullSensor() {
        String groupName = "doesNotMatter";
        Map<String, String> tags = new HashMap<>();
        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), groupName, tags);
        streamsMetrics.removeSensor(null);
    }

    @Test
    public void testRemoveSensor() {
        String groupName = "doesNotMatter";
        String sensorName = "sensor1";
        String scope = "scope";
        String entity = "entity";
        String operation = "put";
        Map<String, String> tags = new HashMap<>();
        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), groupName, tags);

        Sensor sensor1 = streamsMetrics.addSensor(sensorName, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor1);

        Sensor sensor1a = streamsMetrics.addSensor(sensorName, Sensor.RecordingLevel.DEBUG, sensor1);
        streamsMetrics.removeSensor(sensor1a);

        Sensor sensor2 = streamsMetrics.addLatencyAndThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor2);

        Sensor sensor3 = streamsMetrics.addThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor3);
    }

    @Test
    public void testLatencyMetrics() {
        String groupName = "doesNotMatter";
        String scope = "scope";
        String entity = "entity";
        String operation = "put";
        Map<String, String> tags = new HashMap<>();
        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), groupName, tags);

        Sensor sensor1 = streamsMetrics.addLatencyAndThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);

        Map<MetricName, ? extends Metric> metrics = streamsMetrics.metrics();
        // 6 metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
        assertEquals(metrics.size(), 7);

        streamsMetrics.removeSensor(sensor1);
        metrics = streamsMetrics.metrics();
        assertEquals(metrics.size(), 1);
    }

    @Test
    public void testThroughputMetrics() {
        String groupName = "doesNotMatter";
        String scope = "scope";
        String entity = "entity";
        String operation = "put";
        Map<String, String> tags = new HashMap<>();
        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), groupName, tags);

        Sensor sensor1 = streamsMetrics.addThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);

        Map<MetricName, ? extends Metric> metrics = streamsMetrics.metrics();
        // 2 metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
        assertEquals(metrics.size(), 3);

        streamsMetrics.removeSensor(sensor1);
        metrics = streamsMetrics.metrics();
        assertEquals(metrics.size(), 1);
    }
}
