package com.ctrip.hermes.engine;

public interface ConsumerBootstrap {

	public void startConsumer(Subscriber subscriber);

	public void deliverMessage(int correlationId, MessageContext ctx);

}
