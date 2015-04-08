package com.ctrip.hermes.broker.queue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;

import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.service.MessageService;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.ConsumerMessageBatch;
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
	protected ConsumerMessageBatch fetchPriortyMessages(int batchSize) {
		return fetchMessages(batchSize, true);
	}

	@Override
	protected ConsumerMessageBatch fetchNonPriortyMessages(int batchSize) {
		return fetchMessages(batchSize, false);
	}

	private ConsumerMessageBatch fetchMessages(int batchSize, boolean isPriority) {

		try {
			final List<MessagePriority> dataObjs = isPriority ? m_messageService.read(m_priorityTpp, m_priorityOffset,
			      batchSize) : m_messageService.read(m_nonPriorityTpp, m_nonPriorityOffset, batchSize);

			if (dataObjs != null && !dataObjs.isEmpty()) {
				if (isPriority) {
					m_priorityOffset = dataObjs.get(dataObjs.size() - 1).getId();
				} else {
					m_nonPriorityOffset = dataObjs.get(dataObjs.size() - 1).getId();
				}

				final ConsumerMessageBatch batch = new ConsumerMessageBatch();
				for (MessagePriority dataObj : dataObjs) {
					batch.addMsgSeq(dataObj.getId());
				}
				batch.setTopic(m_tpg.getTopic());

				batch.setTransferCallback(new TransferCallback() {

					@Override
					public void transfer(ByteBuf out) {
						for (MessagePriority dataObj : dataObjs) {
							MessageCodec codec = MessageCodecFactory.getCodec(m_tpg.getTopic());
							PartialDecodedMessage partialMsg = new PartialDecodedMessage();
							partialMsg.setAppProperties(stringToByteBuf(dataObj.getAttributes()));
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
