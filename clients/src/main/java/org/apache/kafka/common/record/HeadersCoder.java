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
package org.apache.kafka.common.record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.utils.Utils;

public class HeadersCoder
{
   public byte[] encode(Map<Integer, byte[]> headers){
      byte[] bytes = null;
      if (headers != null && !headers.isEmpty()){
         try {
            bytes = encodeInternal(headers);
         } catch (IOException e) {
            throw new SerializationException("IOException caught encoding headers", e);
         }
      }
      return bytes;
   }


   public Map<Integer, byte[]> decode(byte[] bytes){
      Map<Integer, byte[]> headers = new HashMap<>();
      try {
         if (bytes != null && bytes.length > 0){
            decodeInternal(bytes, headers);
         }
      } catch (IOException e) {
         throw new SerializationException("IOException caught decoding headers", e);
      }
      return headers;
   }

   public void decodeInternal(byte[] bytes, Map<Integer, byte[]> headers) throws IOException
   {

      ByteArrayInputStream bais = null;
      DataInputStream dis = null;

      try
      {
         bais = new ByteArrayInputStream(bytes);
         dis = new DataInputStream(bais);

         while (hasNext(dis))
         {
            int key = dis.readInt();
            int valueLength = dis.readInt();
            byte[] value = new byte[valueLength];
            dis.read(value);
            headers.put(key, value);
         }
      } finally {
         if (dis != null)
            dis.close();
         if (bais != null)
            bais.close();
      }
   }

   public boolean hasNext(InputStream is) throws IOException
   {
      try {
         is.mark(1);
         return is.read() != -1;
      } finally
      {
         is.reset();
      }
   }

   public byte[] encodeInternal(Map<Integer, byte[]> headers) throws IOException
   {
      ByteArrayOutputStream baos = null;
      DataOutputStream dos = null;

      try
      {
         // create byte array output stream
         baos = new ByteArrayOutputStream();

         // create data output stream
         dos = new DataOutputStream(baos);

         // write to the output stream from the buffer
         Collection<Map.Entry<Integer, byte[]>> entrySet;
         if (headers instanceof SortedMap){
            entrySet = headers.entrySet();
         } else {
            entrySet = new TreeMap(headers).entrySet();
         }
         for(Map.Entry<Integer, byte[]> entry : entrySet){
            dos.writeInt(entry.getKey());
            dos.writeInt(entry.getValue().length);
            dos.write(entry.getValue());
         }

         // flushes bytes to underlying output stream
         dos.flush();

         // for each byte in the buffer content
         return baos.toByteArray();
      }finally{

         // releases all system resources from the streams
         if(baos!=null)
            baos.close();
         if(dos!=null)
            dos.close();
      }
   }
}
