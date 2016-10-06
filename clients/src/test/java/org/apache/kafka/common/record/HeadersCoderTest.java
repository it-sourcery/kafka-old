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
package org.apache.kafka.common.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class HeadersCoderTest
{
    HeadersCoder headersCoder = new HeadersCoder();

    @Test
    public void testBasicDeserializationSerialization() {
        Map<Integer, byte[]> original = new HashMap<>();
        original.put(1, "hello".getBytes());
        original.put(2, "hello1".getBytes());
        original.put(3, "hello2".getBytes());

        byte[] bytes = headersCoder.encode(original);

        Map<Integer, byte[]> decoded = headersCoder.decode(bytes);

        assertEquals(original.size(), decoded.size());
        for(Integer key : original.keySet())
        {
            assertTrue(Arrays.equals(original.get(key), decoded.get(key)));
        }
    }

    @Test
    public void testEmptyHeaders() {
        Map<Integer, byte[]> original = new HashMap<>();

        byte[] bytes = headersCoder.encode(original);

        assertTrue(bytes == null);

        Map<Integer, byte[]> decoded = headersCoder.decode(bytes);

        assertEquals(original.size(), decoded.size());
    }

    @Test
    public void testNullHeaders() {
        byte[] bytes = headersCoder.encode(null);

        assertTrue(bytes == null);

        Map<Integer, byte[]> decoded = headersCoder.decode(bytes);

        assertEquals(0, decoded.size());
    }

    @Test
    public void testDeserializationToMaxKeyOnly() {
        Map<Integer, byte[]> original = new HashMap<>();
        original.put(1, "hello".getBytes());
        original.put(2, "hello1".getBytes());
        original.put(3, "hello2".getBytes());

        byte[] bytes = headersCoder.encode(original);

        Map<Integer, byte[]> decoded = headersCoder.decode(bytes, 2);

        assertEquals(3, original.size());
        assertEquals(2, decoded.size());
        assertTrue(Arrays.equals(original.get(1), decoded.get(1)));
        assertTrue(Arrays.equals(original.get(2), decoded.get(2)));
    }

}
