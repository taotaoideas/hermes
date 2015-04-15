package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.partition.MessageQueuePartition;
import com.ctrip.hermes.broker.queue.partition.MessageQueuePartitionCursor;
import com.ctrip.hermes.broker.queue.partition.MessageQueuePartitionFactory;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.google.common.util.concurrent.ListenableFuture;

@Named(type = MessageQueueManager.class)
public class DefaultMessageQueueManager extends ContainerHolder implements MessageQueueManager {

	@Inject
	private MessageQueuePartitionFactory m_queueFactory;

	// one <topic, partition> mapping to one MessageQueue
	private Map<Pair<String, Integer>, MessageQueuePartition> m_messageQueuePartitions = new ConcurrentHashMap<>();

	@Override
	public ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(Tpp tpp, MessageRawDataBatch data) {
		return getMessageQueuePartition(tpp.getTopic(), tpp.getPartition()).appendMessageAsync(tpp.isPriority(), data);
	}

	@Override
	public MessageQueuePartitionCursor createCursor(Tpg tpg) {
		return getMessageQueuePartition(tpg.getTopic(), tpg.getPartition()).createCursor(tpg.getGroupId());
	}

	private MessageQueuePartition getMessageQueuePartition(String topic, int partition) {
		Pair<String, Integer> key = new Pair<>(topic, partition);
		if (!m_messageQueuePartitions.containsKey(key)) {
			synchronized (m_messageQueuePartitions) {
				if (!m_messageQueuePartitions.containsKey(key)) {
					MessageQueuePartition mqp = m_queueFactory.createMessageQueuePartition(topic, partition);
					m_messageQueuePartitions.put(key, mqp);
				}
			}
		}

		return m_messageQueuePartitions.get(key);
	}

	@Override
	public void nack(Tpp tpp, String groupId, boolean resend, List<Long> msgSeqs) {
		getMessageQueuePartition(tpp.getTopic(), tpp.getPartition()).nack(resend, tpp.isPriority(), groupId, msgSeqs);
	}

	@Override
	public void ack(Tpp tpp, String groupId, boolean resend, long msgSeq) {
		getMessageQueuePartition(tpp.getTopic(), tpp.getPartition()).ack(resend, tpp.isPriority(), groupId, msgSeq);
	}
}
