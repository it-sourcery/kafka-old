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
        HeaderProducerRecord<String, String, Integer> headerProducerRecord = new HeaderProducerRecord<String, String, Integer>("test", 1 , "key", "header", 1);
        assertEquals(headerProducerRecord, headerProducerRecord);
        assertEquals(headerProducerRecord.hashCode(), headerProducerRecord.hashCode());

        HeaderProducerRecord<String, String, Integer> equalRecord = new HeaderProducerRecord<String, String, Integer>("test", 1 , "key", "header", 1);
        assertEquals(headerProducerRecord, equalRecord);
        assertEquals(headerProducerRecord.hashCode(), equalRecord.hashCode());

        HeaderProducerRecord<String, String, Integer> topicMisMatch = new HeaderProducerRecord<String, String, Integer>("test-1", 1 , "key", "header", 1);
        assertFalse(headerProducerRecord.equals(topicMisMatch));

        HeaderProducerRecord<String, String, Integer> partitionMismatch = new HeaderProducerRecord<String, String, Integer>("test", 2 , "key", "header", 1);
        assertFalse(headerProducerRecord.equals(partitionMismatch));

        HeaderProducerRecord<String, String, Integer> keyMisMatch = new HeaderProducerRecord<String, String, Integer>("test", 1 , "key-1", "header", 1);
        assertFalse(headerProducerRecord.equals(keyMisMatch));

        HeaderProducerRecord<String, String, Integer> valueMisMatch = new HeaderProducerRecord<String, String, Integer>("test", 1 , "key", "header", 2);
        assertFalse(headerProducerRecord.equals(valueMisMatch));

        HeaderProducerRecord<String, String, Integer> nullFieldsRecord = new HeaderProducerRecord<String, String, Integer>("topic", null, null, null, null, null);
        assertEquals(nullFieldsRecord, nullFieldsRecord);
        assertEquals(nullFieldsRecord.hashCode(), nullFieldsRecord.hashCode());
    }
}
