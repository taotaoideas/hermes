package com.ctrip.hermes.broker.queue;

import java.util.concurrent.TimeUnit;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.ConsumerMessageBatch;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractMessageQueueCursor implements MessageQueueCursor {
	protected Tpg m_tpg;

	protected Tpp m_priorityTpp;

	protected Tpp m_nonPriorityTpp;

	protected long m_priorityOffset;

	protected long m_nonPriorityOffset;

	protected MetaService m_metaService;

	protected int m_groupIdInt;

	public AbstractMessageQueueCursor(Tpg tpg) {
		m_tpg = tpg;
		m_priorityTpp = new Tpp(tpg.getTopic(), tpg.getPartition(), true);
		m_nonPriorityTpp = new Tpp(tpg.getTopic(), tpg.getPartition(), false);
		m_metaService = PlexusComponentLocator.lookup(MetaService.class);

		// TODO
		m_groupIdInt = m_metaService.getGroupIdInt(m_tpg.getGroupId());
	}

	@Override
	public void init() {
		m_priorityOffset = loadPriorityOffset();
		m_nonPriorityOffset = loadNonPriorityOffset();
	}

	protected abstract long loadPriorityOffset();

	protected abstract long loadNonPriorityOffset();

	protected abstract ConsumerMessageBatch fetchPriortyMessages(int batchSize);

	protected abstract ConsumerMessageBatch fetchNonPriortyMessages(int batchSize);

	@Override
	public ConsumerMessageBatch next(int batchSize) {
		while (!Thread.currentThread().isInterrupted()) {
			try {
				ConsumerMessageBatch result = new ConsumerMessageBatch();
				result.setTopic(m_tpg.getTopic());
				ConsumerMessageBatch priorityMessageBatch = fetchPriortyMessages(batchSize);

				if (priorityMessageBatch != null) {
					result.mergeBatch(priorityMessageBatch);
				}

				if (result.size() < batchSize) {
					int remainingBatchSize = batchSize - result.size();
					ConsumerMessageBatch nonPriorityMessageBatch = fetchNonPriortyMessages(remainingBatchSize);

					if (nonPriorityMessageBatch != null) {
						result.mergeBatch(nonPriorityMessageBatch);
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
