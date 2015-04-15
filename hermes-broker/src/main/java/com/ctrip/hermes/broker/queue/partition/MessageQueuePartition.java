package com.ctrip.hermes.broker.queue.partition;

import java.util.List;
import java.util.Map;

import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueuePartition {

	ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(boolean isPriority, MessageRawDataBatch batch);

	MessageQueuePartitionCursor createCursor(String groupId);

	void nack(boolean resend, boolean isPriority, String groupId, List<Long> msgSeqs);

	void ack(boolean resend, boolean isPriority, String groupId, long msgSeq);

}
