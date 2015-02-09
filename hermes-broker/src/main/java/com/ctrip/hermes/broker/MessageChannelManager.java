package com.ctrip.hermes.broker;


public interface MessageChannelManager {

	public void newConsumerChannel(String topic, String groupId, ConsumerChannelHandler handler);

	public ProducerChannel newProducerChannel(String topic);

}
