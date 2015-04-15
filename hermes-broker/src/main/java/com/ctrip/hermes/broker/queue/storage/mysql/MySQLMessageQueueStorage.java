package com.ctrip.hermes.broker.queue.storage.mysql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.google.common.base.Charsets;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class MySQLMessageQueueStorage implements MessageQueueStorage {

	@Override
	public void appendMessages(Tpp tpp, Collection<MessageRawDataBatch> batches) throws Exception {
		List<MessagePriority> msgs = new ArrayList<>();
		for (MessageRawDataBatch batch : batches) {
			List<PartialDecodedMessage> pdmsgs = batch.getMessages();
			for (PartialDecodedMessage pdmsg : pdmsgs) {
				MessagePriority msg = new MessagePriority();
				msg.setAttributes(new String(pdmsg.readDurableProperties(), Charsets.UTF_8));
				msg.setCreationDate(new Date(pdmsg.getBornTime()));
				msg.setPartition(tpp.getPartition());
				msg.setPayload(pdmsg.readBody());
				// TODO
				msg.setPriority(tpp.isPriority() ? 0 : 1);
				// TODO set producer id and producer id in producer
				msg.setProducerId(101);
				msg.setProducerIp("1.1.1.1");
				msg.setRefKey(pdmsg.getKey());
				msg.setTopic(tpp.getTopic());

				msgs.add(msg);
			}
		}

		// TODO
		// m_messageService.write(msgs);
	}

	@Override
	public Object findLastOffset(Tpp tpp, int groupId) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public FetchResult fetchMessages(Tpp tpp, Object startOffset, int batchSize) {
		// try {
		// final List<MessagePriority> dataObjs = isPriority ? m_messageService.read(m_priorityTpp, m_priorityOffset,
		// batchSize) : m_messageService.read(m_nonPriorityTpp, m_nonPriorityOffset, batchSize);
		//
		// if (dataObjs != null && !dataObjs.isEmpty()) {
		// if (isPriority) {
		// m_priorityOffset = dataObjs.get(dataObjs.size() - 1).getId();
		// } else {
		// m_nonPriorityOffset = dataObjs.get(dataObjs.size() - 1).getId();
		// }
		//
		// final TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
		// for (MessagePriority dataObj : dataObjs) {
		// batch.addMsgSeq(dataObj.getId());
		// }
		// batch.setTopic(m_tpg.getTopic());
		// batch.setPartition(m_tpg.getPartition());
		// batch.setPriority(isPriority);
		//
		// batch.setTransferCallback(new TransferCallback() {
		//
		// @Override
		// public void transfer(ByteBuf out) {
		// for (MessagePriority dataObj : dataObjs) {
		// MessageCodec codec = MessageCodecFactory.getCodec(m_tpg.getTopic());
		// PartialDecodedMessage partialMsg = new PartialDecodedMessage();
		// partialMsg.setAppProperties(stringToByteBuf(dataObj.getAttributes()));
		// partialMsg.setBody(Unpooled.wrappedBuffer(dataObj.getPayload()));
		// partialMsg.setBornTime(dataObj.getCreationDate().getTime());
		// partialMsg.setKey(dataObj.getRefKey());
		//
		// codec.encode(partialMsg, out);
		// }
		// }
		// });
		//
		// return batch;
		// }
		//
		// } catch (DalException e) {
		// // TODO
		// }

		return null;
	}

	@Override
	public void nack(Tpp tpp, String groupId, boolean resend, List<Long> msgSeqs) {
		// TODO Auto-generated method stub

	}

	@Override
	public void ack(Tpp tpp, String groupId, boolean resend, long msgSeq) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object findLastResendOffset(Tpg tpg) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public FetchResult fetchResendMessages(Tpg tpg, Object startOffset, int batchSize) {
		// TODO Auto-generated method stub
		return null;
	}

}
