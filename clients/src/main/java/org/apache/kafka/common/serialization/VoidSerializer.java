package org.apache.kafka.common.serialization;

import java.util.Map;

/**
 * Created by pearcem on 9/18/16.
 */
public class VoidSerializer implements Serializer<Void>
{
   @Override
   public void configure(Map<String, ?> configs, boolean isKey)
   {

   }

   @Override
   public byte[] serialize(String topic, Void data)
   {
      return new byte[0];
   }

   @Override
   public void close()
   {

   }
}
