package org.apache.kafka.common.serialization;

import java.util.Map;

/**
 * Created by pearcem on 10/6/16.
 */
public abstract class HeadersAwareSerializer<T> implements Serializer<T>
{

   public byte[] serialize(String topic, Map<Integer, byte[]> headers, T data){
      return this.serialize(topic, data);
   }

}
