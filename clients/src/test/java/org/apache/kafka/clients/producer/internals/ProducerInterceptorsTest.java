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
package org.apache.kafka.clients.producer.internals;


import org.apache.kafka.clients.producer.HeaderProducerRecord;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProducerInterceptorsTest {
    private final TopicPartition tp = new TopicPartition("test", 0);
    private final HeaderProducerRecord<Integer, String, String> headerProducerRecord = new HeaderProducerRecord<>("test", 0, 1, "header","value");
    private int onAckCount = 0;
    private int onErrorAckCount = 0;
    private int onErrorAckWithTopicSetCount = 0;
    private int onErrorAckWithTopicPartitionSetCount = 0;
    private int onSendCount = 0;

    private class AppendProducerInterceptor implements ProducerInterceptor<Integer, String, String> {
        private String appendStr = "";
        private boolean throwExceptionOnSend = false;
        private boolean throwExceptionOnAck = false;

        public AppendProducerInterceptor(String appendStr) {
            this.appendStr = appendStr;
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public HeaderProducerRecord<Integer, String, String> onSend(HeaderProducerRecord<Integer, String, String> record) {
            onSendCount++;
            if (throwExceptionOnSend)
                throw new KafkaException("Injected exception in AppendProducerInterceptor.onSend");

            HeaderProducerRecord<Integer, String, String> newRecord = new HeaderProducerRecord<>(
                    record.topic(), record.partition(), record.key(), record.header(), record.value().concat(appendStr));
            return newRecord;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            onAckCount++;
            if (exception != null) {
                onErrorAckCount++;
                // the length check is just to call topic() method and let it throw an exception
                // if RecordMetadata.TopicPartition is null
                if (metadata != null && metadata.topic().length() >= 0) {
                    onErrorAckWithTopicSetCount++;
                    if (metadata.partition() >= 0)
                        onErrorAckWithTopicPartitionSetCount++;
                }
            }
            if (throwExceptionOnAck)
                throw new KafkaException("Injected exception in AppendProducerInterceptor.onAcknowledgement");
        }

        @Override
        public void close() {
        }

        // if 'on' is true, onSend will always throw an exception
        public void injectOnSendError(boolean on) {
            throwExceptionOnSend = on;
        }

        // if 'on' is true, onAcknowledgement will always throw an exception
        public void injectOnAcknowledgementError(boolean on) {
            throwExceptionOnAck = on;
        }
    }

    @Test
    public void testOnSendChain() {
        List<ProducerInterceptor<Integer, String, String>> interceptorList = new ArrayList<>();
        // we are testing two different interceptors by configuring the same interceptor differently, which is not
        // how it would be done in KafkaProducer, but ok for testing interceptor callbacks
        AppendProducerInterceptor interceptor1 = new AppendProducerInterceptor("One");
        AppendProducerInterceptor interceptor2 = new AppendProducerInterceptor("Two");
        interceptorList.add(interceptor1);
        interceptorList.add(interceptor2);
        ProducerInterceptors<Integer, String, String> interceptors = new ProducerInterceptors<>(interceptorList);

        // verify that onSend() mutates the record as expected
        HeaderProducerRecord<Integer, String, String> interceptedRecord = interceptors.onSend(headerProducerRecord);
        assertEquals(2, onSendCount);
        assertEquals(headerProducerRecord.topic(), interceptedRecord.topic());
        assertEquals(headerProducerRecord.partition(), interceptedRecord.partition());
        assertEquals(headerProducerRecord.key(), interceptedRecord.key());
        assertEquals(interceptedRecord.value(), headerProducerRecord.value().concat("One").concat("Two"));

        // onSend() mutates the same record the same way
        HeaderProducerRecord<Integer, String, String> anotherRecord = interceptors.onSend(headerProducerRecord);
        assertEquals(4, onSendCount);
        assertEquals(interceptedRecord, anotherRecord);

        // verify that if one of the interceptors throws an exception, other interceptors' callbacks are still called
        interceptor1.injectOnSendError(true);
        HeaderProducerRecord<Integer, String, String> partInterceptRecord = interceptors.onSend(headerProducerRecord);
        assertEquals(6, onSendCount);
        assertEquals(partInterceptRecord.value(), headerProducerRecord.value().concat("Two"));

        // verify the record remains valid if all onSend throws an exception
        interceptor2.injectOnSendError(true);
        HeaderProducerRecord<Integer, String, String> noInterceptRecord = interceptors.onSend(headerProducerRecord);
        assertEquals(headerProducerRecord, noInterceptRecord);

        interceptors.close();
    }

    @Test
    public void testOnAcknowledgementChain() {
        List<ProducerInterceptor<Integer, String, String>> interceptorList = new ArrayList<>();
        // we are testing two different interceptors by configuring the same interceptor differently, which is not
        // how it would be done in KafkaProducer, but ok for testing interceptor callbacks
        AppendProducerInterceptor interceptor1 = new AppendProducerInterceptor("One");
        AppendProducerInterceptor interceptor2 = new AppendProducerInterceptor("Two");
        interceptorList.add(interceptor1);
        interceptorList.add(interceptor2);
        ProducerInterceptors<Integer, String, String> interceptors = new ProducerInterceptors<>(interceptorList);

        // verify onAck is called on all interceptors
        RecordMetadata meta = new RecordMetadata(tp, 0, 0, 0, 0, 0, 0, 0);
        interceptors.onAcknowledgement(meta, null);
        assertEquals(2, onAckCount);

        // verify that onAcknowledgement exceptions do not propagate
        interceptor1.injectOnAcknowledgementError(true);
        interceptors.onAcknowledgement(meta, null);
        assertEquals(4, onAckCount);

        interceptor2.injectOnAcknowledgementError(true);
        interceptors.onAcknowledgement(meta, null);
        assertEquals(6, onAckCount);

        interceptors.close();
    }

    @Test
    public void testOnAcknowledgementWithErrorChain() {
        List<ProducerInterceptor<Integer, String, String>> interceptorList = new ArrayList<>();
        AppendProducerInterceptor interceptor1 = new AppendProducerInterceptor("One");
        interceptorList.add(interceptor1);
        ProducerInterceptors<Integer, String, String> interceptors = new ProducerInterceptors<>(interceptorList);

        // verify that metadata contains both topic and partition
        interceptors.onSendError(headerProducerRecord,
                                 new TopicPartition(headerProducerRecord.topic(), headerProducerRecord.partition()),
                                 new KafkaException("Test"));
        assertEquals(1, onErrorAckCount);
        assertEquals(1, onErrorAckWithTopicPartitionSetCount);

        // verify that metadata contains both topic and partition (because record already contains partition)
        interceptors.onSendError(headerProducerRecord, null, new KafkaException("Test"));
        assertEquals(2, onErrorAckCount);
        assertEquals(2, onErrorAckWithTopicPartitionSetCount);

        // if producer record does not contain partition, interceptor should get partition == -1
        HeaderProducerRecord<Integer, String, String> record2 = new HeaderProducerRecord<>("test2", null, 1, "header", "value");
        interceptors.onSendError(record2, null, new KafkaException("Test"));
        assertEquals(3, onErrorAckCount);
        assertEquals(3, onErrorAckWithTopicSetCount);
        assertEquals(2, onErrorAckWithTopicPartitionSetCount);

        // if producer record does not contain partition, but topic/partition is passed to
        // onSendError, then interceptor should get valid partition
        int reassignedPartition = headerProducerRecord.partition() + 1;
        interceptors.onSendError(record2,
                                 new TopicPartition(record2.topic(), reassignedPartition),
                                 new KafkaException("Test"));
        assertEquals(4, onErrorAckCount);
        assertEquals(4, onErrorAckWithTopicSetCount);
        assertEquals(3, onErrorAckWithTopicPartitionSetCount);

        // if both record and topic/partition are null, interceptor should not receive metadata
        interceptors.onSendError(null, null, new KafkaException("Test"));
        assertEquals(5, onErrorAckCount);
        assertEquals(4, onErrorAckWithTopicSetCount);
        assertEquals(3, onErrorAckWithTopicPartitionSetCount);

        interceptors.close();
    }
}

