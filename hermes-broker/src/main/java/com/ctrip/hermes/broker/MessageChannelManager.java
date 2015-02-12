package com.ctrip.hermes.broker;

public interface MessageChannelManager {

	public ConsumerChannel newConsumerChannel(String topic, String groupId);

	public ProducerChannel newProducerChannel(String topic);

}
