package org.apache.kafka.common.serialization;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

/**
 * Created by pearcem on 9/19/16.
 */
public class HeadersStringSerializer implements Serializer<Map<String, String>>
{

   private StringSerializer stringSerializer = new StringSerializer();

   @Override
   public void configure(Map<String, ?> configs, boolean isKey)
   {
      stringSerializer.configure(configs, isKey);
   }

   @Override
   public byte[] serialize(String topic, Map<String, String> data)
   {
      return stringSerializer.serialize(topic, mapToString(data));
   }

   @Override
   public void close()
   {
      stringSerializer.close();
   }

   public static String mapToString(Map<String, String> map) {
      StringBuilder stringBuilder = new StringBuilder();

      for (String key : map.keySet()) {
         if (stringBuilder.length() > 0) {
            stringBuilder.append("&");
         }
         String value = map.get(key);
         try {
            stringBuilder.append((key != null ? URLEncoder.encode(key, "UTF-8") : ""));
            stringBuilder.append("=");
            stringBuilder.append(value != null ? URLEncoder.encode(value, "UTF-8") : "");
         } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("This method requires UTF-8 encoding support", e);
         }
      }

      return stringBuilder.toString();
   }
}
