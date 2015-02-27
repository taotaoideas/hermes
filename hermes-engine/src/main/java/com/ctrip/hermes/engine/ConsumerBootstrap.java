package com.ctrip.hermes.engine;

import java.util.List;

import com.ctrip.hermes.storage.message.Message;

public interface ConsumerBootstrap {

	public void startConsumer(Subscriber subscriber);

	public void deliverMessage(int correlationId, List<Message> msgs);

}
