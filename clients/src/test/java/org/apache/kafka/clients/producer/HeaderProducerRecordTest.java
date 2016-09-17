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
package org.apache.kafka.clients.producer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HeaderProducerRecordTest
{

    @Test
    public void testEqualsAndHashCode() {
        HeaderProducerRecord<String, Integer> headerProducerRecord = new HeaderProducerRecord<String, Integer>("test", 1 , "key", 1);
        assertEquals(headerProducerRecord, headerProducerRecord);
        assertEquals(headerProducerRecord.hashCode(), headerProducerRecord.hashCode());

        HeaderProducerRecord<String, Integer> equalRecord = new HeaderProducerRecord<String, Integer>("test", 1 , "key", 1);
        assertEquals(headerProducerRecord, equalRecord);
        assertEquals(headerProducerRecord.hashCode(), equalRecord.hashCode());

        HeaderProducerRecord<String, Integer> topicMisMatch = new HeaderProducerRecord<String, Integer>("test-1", 1 , "key", 1);
        assertFalse(headerProducerRecord.equals(topicMisMatch));

        HeaderProducerRecord<String, Integer> partitionMismatch = new HeaderProducerRecord<String, Integer>("test", 2 , "key", 1);
        assertFalse(headerProducerRecord.equals(partitionMismatch));

        HeaderProducerRecord<String, Integer> keyMisMatch = new HeaderProducerRecord<String, Integer>("test", 1 , "key-1", 1);
        assertFalse(headerProducerRecord.equals(keyMisMatch));

        HeaderProducerRecord<String, Integer> valueMisMatch = new HeaderProducerRecord<String, Integer>("test", 1 , "key", 2);
        assertFalse(headerProducerRecord.equals(valueMisMatch));

        HeaderProducerRecord<String, Integer> nullFieldsRecord = new HeaderProducerRecord<String, Integer>("topic", null, null, null, null);
        assertEquals(nullFieldsRecord, nullFieldsRecord);
        assertEquals(nullFieldsRecord.hashCode(), nullFieldsRecord.hashCode());
    }
}
