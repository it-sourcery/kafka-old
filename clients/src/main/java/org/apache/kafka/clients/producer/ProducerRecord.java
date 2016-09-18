package org.apache.kafka.clients.producer;

/**
 * Keeps back compatibility
 *
 * @see HeaderProducerRecord
 */
public class ProducerRecord<K, V> extends HeaderProducerRecord<K, Void, V>
{
   public ProducerRecord(String topic, Integer partition, Long timestamp, K key, Void header, V value)
   {
      super(topic, partition, timestamp, key, header, value);
   }

   public ProducerRecord(String topic, Integer partition, K key, Void header, V value)
   {
      super(topic, partition, key, header, value);
   }

   public ProducerRecord(String topic, Integer partition, K key, V value)
   {
      super(topic, partition, key, null, value);
   }

   public ProducerRecord(String topic, K key, Void header, V value)
   {
      super(topic, key, header, value);
   }

   public ProducerRecord(String topic, K key, V value)
   {
      super(topic, key, value);
   }

   public ProducerRecord(String topic, V value)
   {
      super(topic, value);
   }
}
