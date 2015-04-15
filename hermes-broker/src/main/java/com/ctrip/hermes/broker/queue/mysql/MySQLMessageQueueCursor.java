package com.ctrip.hermes.broker.queue.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;

import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.service.MessageService;
import com.ctrip.hermes.broker.queue.AbstractMessageQueueCursor;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.message.codec.MessageCodecFactory;
import com.ctrip.hermes.core.transport.TransferCallback;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.google.common.base.Charsets;

public class MySQLMessageQueueCursor extends AbstractMessageQueueCursor {

	private MessageService m_messageService;

	public MySQLMessageQueueCursor(Tpg tpg) {
		super(tpg);
		m_messageService = PlexusComponentLocator.lookup(MessageService.class);
	}

	@Override
	protected long loadPriorityOffset() {
		try {
			return m_messageService.findLastOffset(m_priorityTpp, m_groupIdInt).getOffset();
		} catch (DalException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(String.format(
			      "Load priority message queue offset failed.(topic=%s, partition=%d, groupId=%d)", m_tpg.getTopic(),
			      m_tpg.getPartition(), m_groupIdInt), e);
		}
	}

	@Override
	protected long loadNonPriorityOffset() {
		try {
			return m_messageService.findLastOffset(m_nonPriorityTpp, m_groupIdInt).getOffset();
		} catch (DalException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(String.format(
			      "Load non-priority message queue offset failed.(topic=%s, partition=%d, groupId=%d)", m_tpg.getTopic(),
			      m_tpg.getPartition(), m_groupIdInt), e);
		}
	}

	@Override
	protected TppConsumerMessageBatch fetchPriortyMessages(int batchSize) {
		return fetchMessages(batchSize, true);
	}

	@Override
	protected TppConsumerMessageBatch fetchNonPriortyMessages(int batchSize) {
		return fetchMessages(batchSize, false);
	}

	private TppConsumerMessageBatch fetchMessages(int batchSize, boolean isPriority) {

		try {
			final List<MessagePriority> dataObjs = isPriority ? m_messageService.read(m_priorityTpp, m_priorityOffset,
			      batchSize) : m_messageService.read(m_nonPriorityTpp, m_nonPriorityOffset, batchSize);

			if (dataObjs != null && !dataObjs.isEmpty()) {
				if (isPriority) {
					m_priorityOffset = dataObjs.get(dataObjs.size() - 1).getId();
				} else {
					m_nonPriorityOffset = dataObjs.get(dataObjs.size() - 1).getId();
				}

				final TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
				for (MessagePriority dataObj : dataObjs) {
					batch.addMsgSeq(dataObj.getId());
				}
				batch.setTopic(m_tpg.getTopic());
				batch.setPartition(m_tpg.getPartition());
				batch.setPriority(true);

				batch.setTransferCallback(new TransferCallback() {

					@Override
					public void transfer(ByteBuf out) {
						for (MessagePriority dataObj : dataObjs) {
							MessageCodec codec = MessageCodecFactory.getCodec(m_tpg.getTopic());
							PartialDecodedMessage partialMsg = new PartialDecodedMessage();
							partialMsg.setDurableProperties(stringToByteBuf(dataObj.getAttributes()));
							partialMsg.setBody(Unpooled.wrappedBuffer(dataObj.getPayload()));
							partialMsg.setBornTime(dataObj.getCreationDate().getTime());
							partialMsg.setKey(dataObj.getRefKey());

							codec.encode(partialMsg, out);
						}
					}
				});

				return batch;
			}

		} catch (DalException e) {
			// TODO
		}

		return null;

	}

	private ByteBuf stringToByteBuf(String str) {
		return Unpooled.wrappedBuffer(str.getBytes(Charsets.UTF_8));
	}

}
