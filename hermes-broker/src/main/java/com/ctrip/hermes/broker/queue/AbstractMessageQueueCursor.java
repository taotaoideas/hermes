package com.ctrip.hermes.broker.queue;

import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.ConsumerMessageBatch;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.TransferCallback;
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
					mergeBatch(priorityMessageBatch, result);
				}

				if (result.getMsgSeqs().size() < batchSize) {
					int remainingBatchSize = batchSize - result.getMsgSeqs().size();
					ConsumerMessageBatch nonPriorityMessageBatch = fetchNonPriortyMessages(remainingBatchSize);

					if (nonPriorityMessageBatch != null) {
						mergeBatch(nonPriorityMessageBatch, result);
					}
				}

				if (!result.getMsgSeqs().isEmpty()) {
					return result;
				}else{
					TimeUnit.MILLISECONDS.sleep(10);
				}
			}catch(InterruptedException e){
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				// TODO
			}
		}

		return null;

	}

	protected void mergeBatch(final ConsumerMessageBatch src, ConsumerMessageBatch target) {
		if (src == null) {
			return;
		} else {
			target.addMsgSeqs(src.getMsgSeqs());
			final TransferCallback originalTransferCallback = target.getTransferCallback();

			target.setTransferCallback(new TransferCallback() {

				@Override
				public void transfer(ByteBuf out) {
					if (originalTransferCallback != null) {
						originalTransferCallback.transfer(out);
					}
					if (src.getTransferCallback() != null) {
						src.getTransferCallback().transfer(out);
					}
				}
			});
		}
	}

}
