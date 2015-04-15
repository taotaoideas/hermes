package com.ctrip.hermes.broker.queue.partition;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage.FetchResult;
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

	protected Object m_priorityOffset;

	protected Object m_nonPriorityOffset;

	protected Object m_resendOffset;

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

	protected abstract Object loadLastPriorityOffset();

	protected abstract Object loadLastNonPriorityOffset();

	protected abstract Object loadLastResendOffset();

	protected abstract FetchResult fetchPriortyMessages(int batchSize);

	protected abstract FetchResult fetchNonPriortyMessages(int batchSize);

	protected abstract FetchResult fetchResendMessages(int batchSize);

	@Override
	public List<TppConsumerMessageBatch> next(int batchSize) {
		while (!Thread.currentThread().isInterrupted()) {
			try {
				List<TppConsumerMessageBatch> result = new LinkedList<>();
				int remainingSize = batchSize;
				FetchResult pFetchResult = fetchPriortyMessages(batchSize);

				if (pFetchResult != null) {
					TppConsumerMessageBatch priorityMessageBatch = pFetchResult.getBatch();
					if (priorityMessageBatch != null && priorityMessageBatch.size() > 0) {
						result.add(priorityMessageBatch);
						remainingSize -= priorityMessageBatch.size();
						m_priorityOffset = pFetchResult.getOffset();
					}
				}

				if (remainingSize > 0) {
					FetchResult rFetchResult = fetchResendMessages(remainingSize);

					if (rFetchResult != null) {
						TppConsumerMessageBatch resendMessageBatch = rFetchResult.getBatch();
						if (resendMessageBatch != null && resendMessageBatch.size() > 0) {
							result.add(resendMessageBatch);
							remainingSize -= resendMessageBatch.size();
							m_resendOffset = rFetchResult.getOffset();
						}
					}
				}

				if (remainingSize > 0) {
					FetchResult npFetchResult = fetchNonPriortyMessages(remainingSize);

					if (npFetchResult != null) {
						TppConsumerMessageBatch nonPriorityMessageBatch = npFetchResult.getBatch();
						if (nonPriorityMessageBatch != null && nonPriorityMessageBatch.size() > 0) {
							result.add(nonPriorityMessageBatch);
							remainingSize -= nonPriorityMessageBatch.size();
							m_nonPriorityOffset = npFetchResult.getOffset();
						}
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
