package com.ctrip.hermes.container;

public interface ConsumerManager {

	public void startConsumer(Subscriber subscriber);

	public void deliverMessage(int correlationId, Object msg);
	
}
