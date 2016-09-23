package org.apache.kafka.common.serialization;

import org.apache.kafka.clients.message.Message;

/**
 * Created by pearcem on 9/20/16.
 */
public interface MessageSerializer<P> extends Serializer<Message<P>>
{
}
