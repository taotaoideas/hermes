package com.ctrip.hermes.broker.queue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.google.common.util.concurrent.ListenableFuture;

@Named(type = MessageQueueManager.class)
public class DefaultMessageQueueManager extends ContainerHolder implements MessageQueueManager {

	private Map<Pair<String, Integer>, MessageQueue> m_messageQueues = new ConcurrentHashMap<>();

	@Override
	public ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(Tpp tpp, MessageRawDataBatch data) {
		return getMessageQueue(tpp.getTopic(), tpp.getPartition()).appendMessageAsync(data, tpp.isPriority());
	}

	@Override
	public MessageQueueCursor createCursor(Tpg tpg) {
		return getMessageQueue(tpg.getTopic(), tpg.getPartition()).createCursor(tpg.getGroupId());
	}

	private MessageQueue getMessageQueue(String topic, int partition) {
		Pair<String, Integer> key = new Pair<>(topic, partition);
		if (!m_messageQueues.containsKey(key)) {
			synchronized (m_messageQueues) {
				if (!m_messageQueues.containsKey(key)) {
					MessageQueue mq = MessageQueueFactory.createMessageQueue(topic, partition);
					m_messageQueues.put(key, mq);
				}
			}
		}

		return m_messageQueues.get(key);
	}
}
