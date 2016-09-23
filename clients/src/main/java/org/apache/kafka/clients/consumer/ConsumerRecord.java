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
package org.apache.kafka.clients.consumer;

import java.util.Map;

import org.apache.kafka.clients.ClientRecord;
import org.apache.kafka.clients.message.Message;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;

/**
 * A key/value pair to be received from Kafka. This consists of a topic name and a partition number, from which the
 * record is being received and an offset that points to the record in a Kafka partition.
 */
public final class ConsumerRecord<K, V> extends ClientRecord<K, V>
{
    public static final long NO_TIMESTAMP = Record.NO_TIMESTAMP;
    public static final int NULL_SIZE = -1;
    public static final int NULL_CHECKSUM = -1;

    private final long offset;
    private final TimestampType timestampType;
    private final long checksum;
    private final int serializedKeySize;
    private final int serializedValueSize;

    /**
     * Creates a record to be received from a specified topic and partition (provided for
     * compatibility with Kafka 0.9 before the message format supported timestamps and before
     * serialized metadata were exposed).
     *
     * @param topic The topic this record is received from
     * @param partition The partition of the topic this record is received from
     * @param offset The offset of this record in the corresponding Kafka partition
     * @param key The key of the record, if one exists (null is allowed)
     * @param value The record contents
     */
    public ConsumerRecord(String topic,
                          int partition,
                          long offset,
                          K key,
                          V value) {
        this(topic, partition, offset, NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                NULL_CHECKSUM, NULL_SIZE, NULL_SIZE, key, value);
    }

    /**
     * Creates a record to be received from a specified topic and partition (provided for
     * compatibility with Kafka 0.10 before the message format supported message headers and payload).
     * *
     * @param topic The topic this record is received from
     * @param partition The partition of the topic this record is received from
     * @param offset The offset of this record in the corresponding Kafka partition
     * @param timestamp The timestamp of the record.
     * @param timestampType The timestamp type
     * @param checksum The checksum (CRC32) of the full record
     * @param serializedKeySize The length of the serialized key
     * @param serializedValueSize The length of the serialized value
     * @param key The key of the record, if one exists (null is allowed)
     * @param value The record contents
     */
    public ConsumerRecord(String topic,
                          int partition,
                          long offset,
                          long timestamp,
                          TimestampType timestampType,
                          long checksum,
                          int serializedKeySize,
                          int serializedValueSize,
                          K key,
                          V value) {
        this(topic, partition, offset, timestamp, timestampType, checksum, serializedKeySize, serializedValueSize, key, new Message<V>(null, value));
    }


    /**
     * Creates a record to be received from a specified topic and partition
     *
     * @param topic The topic this record is received from
     * @param partition The partition of the topic this record is received from
     * @param offset The offset of this record in the corresponding Kafka partition
     * @param timestamp The timestamp of the record.
     * @param timestampType The timestamp type
     * @param checksum The checksum (CRC32) of the full record
     * @param serializedKeySize The length of the serialized key
     * @param serializedValueSize The length of the serialized value
     * @param key The key of the record, if one exists (null is allowed)
     * @param message The message contents
     */
    public ConsumerRecord(String topic,
                          int partition,
                          long offset,
                          long timestamp,
                          TimestampType timestampType,
                          long checksum,
                          int serializedKeySize,
                          int serializedValueSize,
                          K key,
                          Message<V> message) {
        super(topic, partition, timestamp, key, message);
        this.offset = offset;
        this.timestampType = timestampType;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
    }


    /**
     * The position of this record in the corresponding Kafka partition.
     */
    public long offset() {
        return offset;
    }

    /**
     * The timestamp type of this record
     */
    public TimestampType timestampType() {
        return timestampType;
    }

    /**
     * The checksum (CRC32) of the record.
     */
    public long checksum() {
        return this.checksum;
    }

    /**
     * The size of the serialized, uncompressed key in bytes. If key is null, the returned size
     * is -1.
     */
    public int serializedKeySize() {
        return this.serializedKeySize;
    }

    /**
     * The size of the serialized, uncompressed value in bytes. If value is null, the
     * returned size is -1.
     */
    public int serializedValueSize() {
        return this.serializedValueSize;
    }


    @Override
    public String toString()
    {
        return "ConsumerRecord{" +
               "offset=" + offset +
               ", timestampType=" + timestampType +
               ", checksum=" + checksum +
               ", serializedKeySize=" + serializedKeySize +
               ", serializedValueSize=" + serializedValueSize +
               "} " + super.toString();
    }
}
