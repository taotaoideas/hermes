package com.ctrip.hermes.broker.queue.partition;

import java.util.List;

import com.ctrip.hermes.core.message.TppConsumerMessageBatch;

public interface MessageQueuePartitionCursor {

	List<TppConsumerMessageBatch> next(int batchSize);

	void init();

}
