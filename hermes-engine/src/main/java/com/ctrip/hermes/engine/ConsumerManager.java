package com.ctrip.hermes.engine;

import java.util.List;

import com.ctrip.hermes.message.Message;

public interface ConsumerManager {

	public void startConsumer(Subscriber subscriber);

	public void deliverMessage(int correlationId, List<Message<?>> msgs);

}
