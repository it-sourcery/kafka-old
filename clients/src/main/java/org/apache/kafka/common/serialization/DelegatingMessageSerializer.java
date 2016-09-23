package org.apache.kafka.common.serialization;

import java.util.Map;

import org.apache.kafka.clients.message.Headers;
import org.apache.kafka.clients.message.Message;

/**
 * Created by pearcem on 9/20/16.
 */
public class DelegatingMessageSerializer<P> implements MessageSerializer<P>
{

   private Serializer<Headers> headersSerializer;
   private Serializer<P> payloadSerializer;

   @Override
   public void configure(Map<String, ?> properties, boolean isKey)
   {
      DelegatingMessageSerializerConfig configs = new DelegatingMessageSerializerConfig(properties);
      if (headersSerializer == null) {
         this.headersSerializer = configs.getConfiguredInstance(DelegatingMessageSerializerConfig.HEADERS_SERIALIZER_CLASS_CONFIG,
                                                                Serializer.class);
         this.headersSerializer.configure(configs.originals(), true);
      } else {
         configs.ignore(DelegatingMessageSerializerConfig.HEADERS_SERIALIZER_CLASS_CONFIG);
      }
      if (payloadSerializer == null) {
         this.payloadSerializer = configs.getConfiguredInstance(DelegatingMessageSerializerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                                                Serializer.class);
         this.payloadSerializer.configure(configs.originals(), true);
      } else {
         configs.ignore(DelegatingMessageSerializerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
      }
   }

   @Override
   public byte[] serialize(String topic, Message<P> data)
   {
      byte[] headersByteArray = headersSerializer.serialize(topic, data.headers());
      byte[] payloadByteArray = payloadSerializer.serialize(topic, data.payload());

      return new ByteMessage(headersByteArray, payloadByteArray).data();


   }

   @Override
   public void close()
   {
      headersSerializer.close();
      payloadSerializer.close();
   }




}
