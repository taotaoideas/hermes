package com.ctrip.hermes.broker.queue;

import java.util.Map;

import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractMessageQueue implements MessageQueue {

	protected String m_topic;

	protected int m_partition;

	protected MessageQueueDumper m_dumper;

	public AbstractMessageQueue(String topic, int partition) {
		m_topic = topic;
		m_partition = partition;
		m_dumper = getMessageQueueDumper(m_topic, m_partition);
	}

	protected abstract MessageQueueDumper getMessageQueueDumper(String topic, int partition);

	@Override
	public ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(MessageRawDataBatch batch, boolean isPriority) {
		m_dumper.startIfNecessary();

		SettableFuture<Map<Integer, Boolean>> future = SettableFuture.create();

		m_dumper.submit(future, batch, isPriority);

		return future;
	}

	@Override
	public MessageQueueCursor createCursor(String groupId) {
		MessageQueueCursor cursor = doCreateCursor(groupId);
		cursor.init();
		return cursor;
	}

	protected abstract MessageQueueCursor doCreateCursor(String groupId);

}
