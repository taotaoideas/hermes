package com.ctrip.hermes.broker;

public interface ConsumerChannel {

	public void close();

	public void setHandler(ConsumerChannelHandler handler);

	public void open();

}
