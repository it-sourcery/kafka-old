package org.apache.kafka.common.serialization;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by pearcem on 9/19/16.
 */
public class HeadersStringDeserializer implements Deserializer<Map<String, String>>
{

   private StringDeserializer stringDeserializer = new StringDeserializer();

   @Override
   public void configure(Map<String, ?> configs, boolean isKey)
   {
      stringDeserializer.configure(configs, isKey);
   }

   @Override
   public Map<String, String> deserialize(String topic, byte[] data)
   {
      return stringToMap(stringDeserializer.deserialize(topic, data));
   }

   @Override
   public void close()
   {
      stringDeserializer.close();
   }

   public static Map<String, String> stringToMap(String input) {
      Map<String, String> map = new HashMap<String, String>();

      String[] nameValuePairs = input.split("&");
      for (String nameValuePair : nameValuePairs) {
         String[] nameValue = nameValuePair.split("=");
         try {
            map.put(URLDecoder.decode(nameValue[0], "UTF-8"), nameValue.length > 1 ? URLDecoder.decode(nameValue[1], "UTF-8") : "");
         } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("This method requires UTF-8 encoding support", e);
         }
      }

      return map;
   }
}
