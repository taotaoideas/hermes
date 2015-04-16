package com.ctrip.hermes.broker.queue.partition;

import java.util.List;
import java.util.Map;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractMessageQueuePartition implements MessageQueuePartition {

	protected String m_topic;

	protected int m_partition;

	protected MessageQueuePartitionDumper m_dumper;

	protected MessageQueueStorage m_storage;

	public AbstractMessageQueuePartition(String topic, int partition, MessageQueueStorage storage) {
		m_topic = topic;
		m_partition = partition;
		m_storage = storage;
		m_dumper = getMessageQueuePartitionDumper();
	}

	@Override
	public ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(boolean isPriority, MessageRawDataBatch batch) {
		m_dumper.startIfNecessary();

		SettableFuture<Map<Integer, Boolean>> future = SettableFuture.create();

		m_dumper.submit(future, batch, isPriority);

		return future;
	}

	@Override
	public MessageQueuePartitionCursor createCursor(String groupId) {
		MessageQueuePartitionCursor cursor = doCreateCursor(groupId);
		cursor.init();
		return cursor;
	}

	@Override
	public void nack(boolean resend, boolean isPriority, String groupId, List<Long> msgSeqs) {
		doNack(resend, isPriority, groupId, msgSeqs);
	}

	@Override
	public void ack(boolean resend, boolean isPriority, String groupId, long msgSeq) {
		doAck(resend, isPriority, groupId, msgSeq);
	}

	protected abstract MessageQueuePartitionDumper getMessageQueuePartitionDumper();

	protected abstract MessageQueuePartitionCursor doCreateCursor(String groupId);

	protected abstract void doNack(boolean resend, boolean isPriority, String groupId, List<Long> msgSeqs);

	protected abstract void doAck(boolean resend, boolean isPriority, String groupId, long msgSeq);
}
