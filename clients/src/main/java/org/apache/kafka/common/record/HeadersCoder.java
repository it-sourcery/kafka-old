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

/**
 * Created by pearcem on 9/29/16.
 */
public class HeadersCoder
{
   public byte[] encode(Map<Integer, byte[]> headers){
      byte[] bytes = null;
      if (headers != null && headers.size() > 0){
         try {
            bytes = encodeInternal(headers);
         } catch (IOException e) {
            throw new SerializationException("IOException caught encoding headers", e);
         }
      }
      return bytes;
   }

   public Map<Integer, byte[]> decode(byte[] bytes) {
      return decode(bytes, Integer.MAX_VALUE);
   }

   public Map<Integer, byte[]> decode(byte[] bytes, int maxHeaderKey){
      Map<Integer, byte[]> headers = new HashMap<>();
      try {
         if (bytes != null && bytes.length > 0){
            decodeInternal(bytes, maxHeaderKey, headers);
         }
      } catch (IOException e) {
         throw new SerializationException("IOException caught decoding headers", e);
      }
      return headers;
   }

   public void decodeInternal(byte[] bytes, int maxHeaderKey, Map<Integer, byte[]> headers) throws IOException
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
            if (key > maxHeaderKey){
               break;
            }
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

   public byte[] toByteArray(Integer key, byte[] bytes){
      ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + bytes.length);
      byteBuffer.putInt(key);
      byteBuffer.putInt(bytes.length);
      byteBuffer.put(bytes);
      return byteBuffer.array();
   }


}
