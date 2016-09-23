/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer;

import java.util.Map;

import org.apache.kafka.clients.ClientRecord;
import org.apache.kafka.clients.message.Message;

/**
 * A key/value pair to be sent to Kafka. This consists of a topic name to which the record is being sent, an optional
 * partition number, and an optional key and value.
 * <p>
 * If a valid partition number is specified that partition will be used when sending the record. If no partition is
 * specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is
 * present a partition will be assigned in a round-robin fashion.
 * <p>
 * The record also has an associated timestamp. If the user did not provide a timestamp, the producer will stamp the
 * record with its current time. The timestamp eventually used by Kafka depends on the timestamp type configured for
 * the topic.
 * <li>
 * If the topic is configured to use {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime},
 * the timestamp in the producer record will be used by the broker.
 * </li>
 * <li>
 * If the topic is configured to use {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime},
 * the timestamp in the producer record will be overwritten by the broker with the broker local time when it appends the
 * message to its log.
 * </li>
 * <p>
 * In either of the cases above, the timestamp that has actually been used will be returned to user in
 * {@link RecordMetadata}
 */
public final class ProducerRecord<K, V> extends ClientRecord<K, V>{

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     * 
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param timestamp The timestamp of the record
     * @param key The key that will be included in the record
     * @param message The message contents
     */
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, Message<V> message) {
        super(topic, partition, timestamp, key, message);
    }

    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value)
    {
        this(topic, partition, timestamp, key, new Message<>(null, value));
    }

    /**
     * Creates a record to be sent to a specified topic and partition
     *
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param key The key that will be included in the record
     * @param message The message contents
     */
    public ProducerRecord(String topic, Integer partition, K key,  Message<V> message)
    {
        this(topic, partition, null, key, message);
    }

    public ProducerRecord(String topic, Integer partition, K key, V value) {
        this(topic, partition, null, key, new Message<V>(null, value));
    }

    /**
     * Create a record to be sent to Kafka
     * 
     * @param topic The topic the record will be appended to
     * @param key The key that will be included in the record
     * @param message The message contents
     */
    public ProducerRecord(String topic, K key, Message<V> message)
    {
        this(topic, null, null, key, message);
    }

    public ProducerRecord(String topic, K key, V value) {
        this(topic, null, null, key, new Message<>(null, value));
    }

    public ProducerRecord(String topic, Message<V> message)
    {
        this(topic, null, null, null, message);
    }

    public ProducerRecord(String topic, V value) {
        this(topic, null, null, null, new Message<>(null, value));
    }

}
