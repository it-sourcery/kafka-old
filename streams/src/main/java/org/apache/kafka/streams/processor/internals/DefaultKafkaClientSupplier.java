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

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.apache.kafka.streams.KafkaClientSupplier;

public class DefaultKafkaClientSupplier implements KafkaClientSupplier {
    @Override
    public Producer<byte[], Void, byte[]> getProducer(Map<String, Object> config) {
        return new KafkaProducer<>(config, new ByteArraySerializer(), new VoidSerializer(), new ByteArraySerializer());
    }

    @Override
    public Consumer<byte[], Void, byte[]> getConsumer(Map<String, Object> config) {
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new VoidDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public Consumer<byte[], Void, byte[]> getRestoreConsumer(Map<String, Object> config) {
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new VoidDeserializer(), new ByteArrayDeserializer());
    }
}
