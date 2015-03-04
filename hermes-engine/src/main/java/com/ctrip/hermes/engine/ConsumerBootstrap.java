package com.ctrip.hermes.engine;

import java.util.List;

import com.ctrip.hermes.storage.message.Record;

public interface ConsumerBootstrap {

	public void startConsumer(Subscriber subscriber);

	public void deliverMessage(int correlationId, List<Record> msgs);

}
