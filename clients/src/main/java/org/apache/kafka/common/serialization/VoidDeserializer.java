package org.apache.kafka.common.serialization;

import java.util.Map;

/**
 * Created by pearcem on 9/18/16.
 */
public class VoidDeserializer implements Deserializer<Void>
{
   @Override
   public void configure(Map<String, ?> configs, boolean isKey)
   {

   }

   @Override
   public Void deserialize(String topic, byte[] data)
   {
      return null;
   }

   @Override
   public void close()
   {

   }
}
