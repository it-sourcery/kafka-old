package org.apache.kafka.clients;

import org.apache.kafka.clients.message.Message;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by pearcem on 9/20/16.
 */
public class ClientRecord<K, P>
{
   private final String topic;
   private final Integer partition;
   private final K key;
   private final Message<P> message;
   private final Long timestamp;


   public ClientRecord(String topic, Integer partition, Long timestamp, K key, Message<P> message)
   {
      if (topic == null)
         throw new IllegalArgumentException("Topic cannot be null");
      if (timestamp != null && timestamp < 0)
         throw new IllegalArgumentException("Invalid timestamp " + timestamp);
      this.topic = topic;
      this.partition = partition;
      this.key = key;
      this.message = message;
      this.timestamp = timestamp;
   }

   /**
    * @return The topic this record is being sent to
    */
   public String topic() {
      return topic;
   }

   /**
    * @return The key (or null if no key is specified)
    */
   public K key() {
      return key;
   }

   /**
    * @return The value
    */
   public P value() {
      return message.payload();
   }

   /**
    * @return The message headers and payload
    */
   public Message<P> message(){
      return message;
   }

   /**
    * @return The timestamp
    */
   public Long timestamp() {
      return timestamp;
   }

   /**
    * @return The partition to which the record will be sent (or null if no partition was specified)
    */
   public Integer partition() {
      return partition;
   }


   @Override
   public String toString() {
      String key = this.key == null ? "null" : this.key.toString();
      String message = this.message == null ? "null" : this.message.toString();
      String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();

      return "ClientRecord(topic=" + topic + ", partition=" + partition + ", key=" + key + ", message=" + message +
             ", timestamp=" + timestamp + ")";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      else if (!(o instanceof ProducerRecord))
         return false;

      ClientRecord<?, ?> that = (ClientRecord<?, ?>) o;

      if (key != null ? !key.equals(that.key) : that.key != null)
         return false;
      else if (partition != null ? !partition.equals(that.partition) : that.partition != null)
         return false;
      else if (topic != null ? !topic.equals(that.topic) : that.topic != null)
         return false;
      else if (message != null ? !message.equals(that.message) : that.message != null)
         return false;
      else if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null)
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      int result = topic != null ? topic.hashCode() : 0;
      result = 31 * result + (partition != null ? partition.hashCode() : 0);
      result = 31 * result + (key != null ? key.hashCode() : 0);
      result = 31 * result + (message != null ? message.hashCode() : 0);
      result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
      return result;
   }
}
