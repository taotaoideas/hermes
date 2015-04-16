package com.ctrip.hermes.broker.queue.partition;

import java.util.List;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultMessageQueuePartition extends AbstractMessageQueuePartition {

	private MetaService m_metaService;

	public DefaultMessageQueuePartition(String topic, int partition, MessageQueueStorage storage, MetaService metaService) {
		super(topic, partition, storage);
		m_metaService = metaService;
	}

	@Override
	protected MessageQueuePartitionDumper getMessageQueuePartitionDumper() {
		return new DefaultMessageQueuePartitionDumper(m_topic, m_partition, m_storage);
	}

	@Override
	protected MessageQueuePartitionCursor doCreateCursor(String groupId) {
		return new DefaultMessageQueuePartitionCursor(new Tpg(m_topic, m_partition, groupId), m_storage, m_metaService);
	}

	@Override
	protected void doNack(boolean resend, boolean isPriority, String groupId, List<Pair<Long, Integer>> msgSeqs) {
		m_storage.nack(new Tpp(m_topic, m_partition, isPriority), groupId, resend, msgSeqs);
	}

	@Override
	protected void doAck(boolean resend, boolean isPriority, String groupId, long msgSeq) {
		m_storage.ack(new Tpp(m_topic, m_partition, isPriority), groupId, resend, msgSeq);
	}

}
