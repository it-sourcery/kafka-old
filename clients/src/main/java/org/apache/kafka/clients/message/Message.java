package org.apache.kafka.clients.message;

import java.util.Map;

/**
 * Created by pearcem on 9/20/16.
 */
public class Message<P>
{
   private Headers headers;
   private P payload;

   public Message(Headers headers, P payload){
      this.headers = headers;
      this.payload = payload;
   }

   public Headers headers(){
      return this.headers;
   }

   public P payload(){
      return this.payload;
   }

   @Override
   public boolean equals(Object o)
   {
      if (this == o)
      {
         return true;
      }
      if (o == null || getClass() != o.getClass())
      {
         return false;
      }

      Message<?> message = (Message<?>) o;

      if (headers != null ? !headers.equals(message.headers) : message.headers != null)
      {
         return false;
      }
      return payload != null ? payload.equals(message.payload) : message.payload == null;

   }

   @Override
   public int hashCode()
   {
      int result = headers != null ? headers.hashCode() : 0;
      result = 31 * result + (payload != null ? payload.hashCode() : 0);
      return result;
   }

   @Override
   public String toString()
   {
      return "Message{" +
             "headers=" + headers +
             ", payload=" + payload +
             '}';
   }
}
