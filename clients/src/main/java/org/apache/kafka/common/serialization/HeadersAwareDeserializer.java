package org.apache.kafka.common.serialization;

import java.util.Map;

/**
 * Created by pearcem on 10/6/16.
 */
public abstract class HeadersAwareDeserializer<T> implements Deserializer<T>
{

   public T deserialize(String topic, Map<Integer, byte[]> headers, byte[] data){
      return this.deserialize(topic, data);
   }

}
