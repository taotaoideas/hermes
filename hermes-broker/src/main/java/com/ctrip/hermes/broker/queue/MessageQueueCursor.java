package com.ctrip.hermes.broker.queue;

import java.util.List;

import com.ctrip.hermes.core.message.TppConsumerMessageBatch;

public interface MessageQueueCursor {

	List<TppConsumerMessageBatch> next(int batchSize);

	void init();

}
