package com.ctrip.hermes.engine;

import java.util.List;

import com.ctrip.hermes.message.StoredMessage;

public interface ConsumerBootstrap {

	public void startConsumer(Subscriber subscriber);

	public void deliverMessage(int correlationId, List<StoredMessage<byte[]>> msgs);

}
