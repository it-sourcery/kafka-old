package org.apache.kafka.common.serialization;

import java.util.Map;

import org.apache.kafka.clients.message.Headers;
import org.apache.kafka.clients.message.Message;

/**
 * Created by pearcem on 9/20/16.
 */
public class DelegatingMessageDeserializer<P> implements MessageDeserializer<P>
{
   public static final int INT_LENGTH = 4;
   private Deserializer<Headers> headersDeserializer;
   private Deserializer<P> payloadDeserializer;

   @Override
   public void configure(Map<String, ?> properties, boolean isKey)
   {
      DelegatingMessageDeserializerConfig configs = new DelegatingMessageDeserializerConfig(properties);
      if (headersDeserializer == null) {
         this.headersDeserializer = configs.getConfiguredInstance(DelegatingMessageDeserializerConfig.HEADERS_DESERIALIZER_CLASS_CONFIG,
                                                                  Deserializer.class);
         this.headersDeserializer.configure(configs.originals(), true);
      } else {
         configs.ignore(DelegatingMessageDeserializerConfig.HEADERS_DESERIALIZER_CLASS_CONFIG);
      }
      if (payloadDeserializer == null) {
         this.payloadDeserializer = configs.getConfiguredInstance(DelegatingMessageDeserializerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                                                  Deserializer.class);
         this.payloadDeserializer.configure(configs.originals(), true);
      } else {
         configs.ignore(DelegatingMessageDeserializerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
      }
   }

   @Override
   public Message<P> deserialize(String topic, byte[] data)
   {
      ByteMessage byteMessage = new ByteMessage(data);

      Headers headers = headersDeserializer.deserialize(topic, byteMessage.headers().array());
      P payload = payloadDeserializer.deserialize(topic, byteMessage.payload().array());

      return new Message<>(headers, payload);
   }

   @Override
   public void close()
   {
      headersDeserializer.close();
      payloadDeserializer.close();
   }
}
