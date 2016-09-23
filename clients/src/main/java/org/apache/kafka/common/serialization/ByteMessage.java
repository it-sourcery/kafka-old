package org.apache.kafka.common.serialization;

import java.nio.ByteBuffer;

/**
 * Created by pearcem on 9/21/16.
 */
public class ByteMessage
{

   public static final int VERSION_LENGTH = 1;
   public static final byte VERSION = 0;
   public static final int HEADERS_SIZE_LENGTH = 4;
   public static final int PAYLOAD_SIZE_LENGTH = 4;

   private ByteBuffer data;

   public ByteMessage(byte[] headers, byte[] payload){
      this.data = toByteBuffer(headers, payload);
      this.data.rewind();
   }

   public ByteMessage(byte[] data){
      this.data = ByteBuffer.wrap(data);
      this.data.rewind();
   }


   public byte version(){
      return data.get(0);
   }


   private int headersOffset(){
      return VERSION_LENGTH;
   }

   private int headersLength(){
      return data.getInt(headersLength());
   }

   public ByteBuffer headers(){
      return sliceDelimited(data, headersOffset());
   }


   private int payloadOffset(){
      return VERSION_LENGTH + HEADERS_SIZE_LENGTH + headersLength();
   }

   private int payloadLength(){
      return data.getInt(payloadOffset());
   }

   public ByteBuffer payload(){
      return sliceDelimited(data, payloadOffset());
   }

   public byte[] data(){
      data.rewind();
      return data.array();
   }


   public ByteBuffer toByteBuffer(byte[] headers, byte[] payload){

      int size = VERSION_LENGTH + HEADERS_SIZE_LENGTH + PAYLOAD_SIZE_LENGTH;
      size += headers == null ? 0 : headers.length;
      size += payload == null ? 0 : payload.length;

      ByteBuffer byteBuffer = ByteBuffer.allocate(size);
      byteBuffer.put(VERSION);
      put(byteBuffer, headers);
      put(byteBuffer, payload);

      return byteBuffer;
   }

   public void put(ByteBuffer byteBuffer, byte[] array){
      if (array == null){
         byteBuffer.putInt(-1);
      } else {
         byteBuffer.putInt(array.length);
         byteBuffer.put(array);
      }
   }

   /**
    * Read a size-delimited byte buffer starting at the given offset
    */
   private ByteBuffer sliceDelimited(ByteBuffer buffer, int start) {
      int size = buffer.getInt(start);
      if(size < 0) {
         return null;
      } else {
         ByteBuffer b = buffer.duplicate();
         b.position(start + 4);
         b = b.slice();
         b.limit(size);
         b.rewind();
         return b;
      }
   }
}
