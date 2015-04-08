package com.ctrip.hermes.broker.queue;

import com.ctrip.hermes.core.message.ConsumerMessageBatch;

public interface MessageQueueCursor {

	ConsumerMessageBatch next(int batchSize);

	void init();

}
