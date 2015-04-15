package com.ctrip.hermes.broker.queue.partition;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractMessageQueuePartitionCursor implements MessageQueuePartitionCursor {
	protected Tpg m_tpg;

	protected Tpp m_priorityTpp;

	protected Tpp m_nonPriorityTpp;

	protected long m_priorityOffset;

	protected long m_nonPriorityOffset;

	protected long m_resendOffset;

	protected MetaService m_metaService;

	protected int m_groupIdInt;

	public AbstractMessageQueuePartitionCursor(Tpg tpg, MetaService metaService) {
		m_tpg = tpg;
		m_priorityTpp = new Tpp(tpg.getTopic(), tpg.getPartition(), true);
		m_nonPriorityTpp = new Tpp(tpg.getTopic(), tpg.getPartition(), false);
		m_metaService = metaService;

		// TODO
		m_groupIdInt = m_metaService.getGroupIdInt(m_tpg.getGroupId());
	}

	@Override
	public void init() {
		m_priorityOffset = loadLastPriorityOffset();
		m_nonPriorityOffset = loadLastNonPriorityOffset();
		m_resendOffset = loadLastResendOffset();
	}

	protected abstract long loadLastPriorityOffset();

	protected abstract long loadLastNonPriorityOffset();

	protected abstract long loadLastResendOffset();

	protected abstract TppConsumerMessageBatch fetchPriortyMessages(int batchSize);

	protected abstract TppConsumerMessageBatch fetchNonPriortyMessages(int batchSize);

	protected abstract TppConsumerMessageBatch fetchResendMessages(int batchSize);

	@Override
	public List<TppConsumerMessageBatch> next(int batchSize) {
		while (!Thread.currentThread().isInterrupted()) {
			try {
				List<TppConsumerMessageBatch> result = new LinkedList<>();
				int remainingSize = batchSize;
				TppConsumerMessageBatch priorityMessageBatch = fetchPriortyMessages(batchSize);

				if (priorityMessageBatch != null) {
					result.add(priorityMessageBatch);
					remainingSize -= priorityMessageBatch.size();
				}

				if (remainingSize > 0) {
					TppConsumerMessageBatch resendMessageBatch = fetchResendMessages(remainingSize);

					if (resendMessageBatch != null) {
						result.add(resendMessageBatch);
						remainingSize -= resendMessageBatch.size();
					}
				}

				if (remainingSize > 0) {
					TppConsumerMessageBatch nonPriorityMessageBatch = fetchNonPriortyMessages(remainingSize);

					if (nonPriorityMessageBatch != null) {
						result.add(nonPriorityMessageBatch);
						remainingSize -= nonPriorityMessageBatch.size();
					}
				}

				if (result.size() > 0) {
					return result;
				} else {
					TimeUnit.MILLISECONDS.sleep(10);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				// TODO
			}
		}

		return null;

	}

}
