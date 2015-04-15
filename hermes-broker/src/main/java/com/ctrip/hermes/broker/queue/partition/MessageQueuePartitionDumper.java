package com.ctrip.hermes.broker.queue.partition;

import java.util.Map;

import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueuePartitionDumper {

	void startIfNecessary();

	void submit(SettableFuture<Map<Integer, Boolean>> future, MessageRawDataBatch batch, boolean isPriority);

}
