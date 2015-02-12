package com.ctrip.hermes.broker;

public interface ConsumerChannel {

	public void close();

	public void start(ConsumerChannelHandler handler);

	public void ack();

}
