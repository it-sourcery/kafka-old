package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.record.TimestampType;

/**
 * Created by pearcem on 9/17/16.
 */
public class ConsumerRecord<K, V> extends HeaderConsumerRecord<K, Object, V>
{
   public ConsumerRecord(String topic, int partition, long offset, K key, V value)
   {
      super(topic, partition, offset, key, value);
   }

   public ConsumerRecord(String topic, int partition, long offset, long timestamp,
                         TimestampType timestampType, long checksum, int serializedKeySize,
                         int serializedValueSize, K key, V value)
   {
      super(topic, partition, offset, timestamp, timestampType, checksum, serializedKeySize, serializedValueSize, key, value);
   }

   public ConsumerRecord(String topic, int partition, long offset, long timestamp,
                         TimestampType timestampType, long checksum, int serializedKeySize, int serializedHeaderSize,
                         int serializedValueSize, K key, Object header, V value)
   {
      super(topic, partition, offset, timestamp, timestampType, checksum, serializedKeySize, serializedHeaderSize, serializedValueSize, key,
            header, value);
   }
}
