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
package org.apache.kafka.streams.tests;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ShutdownDeadlockTest {

    private final String kafka;

    public ShutdownDeadlockTest(final String kafka) {
        this.kafka = kafka;
    }

    public void start() {
        final String topic = "source";
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "shouldNotDeadlock");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, String> source = builder.stream(Serdes.String(), Serdes.String(), topic);

        source.foreach(new ForeachAction<String, String>() {
            @Override
            public void apply(final String key, final String value) {
                throw new RuntimeException("KABOOM!");
            }
        });
        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread t, final Throwable e) {
                System.exit(-1);
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close(5, TimeUnit.SECONDS);
            }
        }));

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "SmokeTest");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        producer.send(new ProducerRecord<>(topic, "a", "a"));
        producer.flush();

        streams.start();

        synchronized (this) {
            try {
                wait();
            } catch (InterruptedException e) {
                // ignored
            }
        }


    }



}
