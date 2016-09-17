package org.apache.kafka.clients.consumer;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

/**
 * Created by pearcem on 9/17/16.
 */
public class ConsumerRecords<K, V> extends HeaderConsumerRecords<K, Object, V>
{
   public ConsumerRecords(Map<TopicPartition, List<HeaderConsumerRecord<K, Object, V>>> records)
   {
      super(records);
   }
}
