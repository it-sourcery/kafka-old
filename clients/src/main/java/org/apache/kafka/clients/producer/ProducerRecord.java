package org.apache.kafka.clients.producer;

/**
 * Keeps back compatibility
 *
 * @see HeaderProducerRecord
 */
public class ProducerRecord<K, V> extends HeaderProducerRecord<K, Object, V>
{
   public ProducerRecord(String topic, Integer partition, Long timestamp, K key, Object header, V value)
   {
      super(topic, partition, timestamp, key, header, value);
   }

   public ProducerRecord(String topic, Integer partition, K key, Object header, V value)
   {
      super(topic, partition, key, header, value);
   }

   public ProducerRecord(String topic, Integer partition, K key, V value)
   {
      super(topic, partition, key, value);
   }

   public ProducerRecord(String topic, K key, Object header, V value)
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
