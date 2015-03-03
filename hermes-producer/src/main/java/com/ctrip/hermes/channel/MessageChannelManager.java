package com.ctrip.hermes.channel;

public interface MessageChannelManager {

	public ConsumerChannel newConsumerChannel(String topic, String groupId, String partition);
	
	public ConsumerChannel newConsumerChannel(String topic, String groupId);

	public ProducerChannel newProducerChannel(String topic);

}
