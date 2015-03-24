package com.ctrip.hermes.channel;

import com.ctrip.hermes.meta.entity.Endpoint;

public interface MessageChannelManager {

	public ConsumerChannel newConsumerChannel(String topic, String groupId, String partition);

	public ConsumerChannel newConsumerChannel(String topic, String groupId);

	public ProducerChannel getProducerChannel(Endpoint endpoint);

}
