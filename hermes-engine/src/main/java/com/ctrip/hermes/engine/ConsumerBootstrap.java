package com.ctrip.hermes.engine;

import java.util.List;

import com.ctrip.hermes.message.StoredMessage;

public interface ConsumerBootstrap {

	public void startConsumer(Subscriber subscriber);

	// TODO remove this method and move to some handler return from startConsumer()
	public void deliverMessage(long correlationId, List<StoredMessage<byte[]>> msgs);

}
