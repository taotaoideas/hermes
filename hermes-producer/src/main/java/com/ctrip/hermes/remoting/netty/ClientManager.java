package com.ctrip.hermes.remoting.netty;

public interface ClientManager {

	public NettyClientHandler findProducerClient(String topic);

	public NettyClientHandler findConsumerClient(String topicPattern, String groupId);

}