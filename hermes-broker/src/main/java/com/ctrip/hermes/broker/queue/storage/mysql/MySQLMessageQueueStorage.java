package com.ctrip.hermes.broker.queue.storage.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.dal.hermes.DeadLetter;
import com.ctrip.hermes.broker.dal.hermes.DeadLetterDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityEntity;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessage;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessageDao;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessageEntity;
import com.ctrip.hermes.broker.dal.hermes.OffsetResend;
import com.ctrip.hermes.broker.dal.hermes.OffsetResendDao;
import com.ctrip.hermes.broker.dal.hermes.OffsetResendEntity;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupId;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupIdDao;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupIdEntity;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.RetryPolicy;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.message.codec.MessageCodecFactory;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.TransferCallback;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.meta.entity.Storage;
import com.google.common.base.Charsets;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MessageQueueStorage.class, value = Storage.MYSQL)
public class MySQLMessageQueueStorage implements MessageQueueStorage {

	@Inject
	private MessagePriorityDao m_msgDao;

	@Inject
	private ResendGroupIdDao m_resendDao;

	@Inject
	private OffsetResendDao m_offsetResendDao;

	@Inject
	private OffsetMessageDao m_offsetMessageDao;

	@Inject
	private DeadLetterDao m_deadLetterDao;

	@Inject
	private MetaService m_metaService;

	@Override
	public void appendMessages(Tpp tpp, Collection<MessageRawDataBatch> batches) throws Exception {
		List<MessagePriority> msgs = new ArrayList<>();
		for (MessageRawDataBatch batch : batches) {
			List<PartialDecodedMessage> pdmsgs = batch.getMessages();
			for (PartialDecodedMessage pdmsg : pdmsgs) {
				MessagePriority msg = new MessagePriority();
				msg.setAttributes(new String(pdmsg.readDurableProperties(), Charsets.ISO_8859_1));
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

		m_msgDao.insert(msgs.toArray(new MessagePriority[msgs.size()]));
	}

	@Override
	public synchronized Object findLastOffset(Tpp tpp, int groupId) throws Exception {
		String topic = tpp.getTopic();
		int partition = tpp.getPartition();
		int priority = tpp.getPriorityInt();

		List<OffsetMessage> lastOffset = m_offsetMessageDao.find(topic, partition, priority, groupId,
		      OffsetMessageEntity.READSET_FULL);

		if (lastOffset.isEmpty()) {
			List<MessagePriority> topMsg = m_msgDao.top(topic, partition, priority, MessagePriorityEntity.READSET_FULL);

			long startOffset = 0L;
			if (!topMsg.isEmpty()) {
				startOffset = CollectionUtil.last(topMsg).getId();
			}

			OffsetMessage offset = new OffsetMessage();
			offset.setCreationDate(new Date());
			offset.setGroupId(groupId);
			offset.setOffset(startOffset);
			offset.setPartition(partition);
			offset.setPriority(priority);
			offset.setTopic(topic);

			m_offsetMessageDao.insert(offset);
			return offset.getOffset();
		} else {
			return CollectionUtil.last(lastOffset).getOffset();
		}
	}

	@Override
	public FetchResult fetchMessages(Tpp tpp, Object startOffset, int batchSize) {
		FetchResult result = new FetchResult();
		try {
			final List<MessagePriority> dataObjs = m_msgDao.findIdAfter(tpp.getTopic(), tpp.getPartition(),
			      tpp.getPriorityInt(), (Long) startOffset, batchSize, MessagePriorityEntity.READSET_FULL);

			long biggestOffset = 0L;
			if (dataObjs != null && !dataObjs.isEmpty()) {
				final TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
				for (MessagePriority dataObj : dataObjs) {
					biggestOffset = Math.max(biggestOffset, dataObj.getId());
					batch.addMsgSeq(dataObj.getId(), 0);
				}
				final String topic = tpp.getTopic();
				batch.setTopic(topic);
				batch.setPartition(tpp.getPartition());
				batch.setPriority(tpp.isPriority());

				batch.setTransferCallback(new TransferCallback() {

					@Override
					public void transfer(ByteBuf out) {
						for (MessagePriority dataObj : dataObjs) {
							MessageCodec codec = MessageCodecFactory.getCodec(topic);
							PartialDecodedMessage partialMsg = new PartialDecodedMessage();
							partialMsg.setRemainingRetries(0);
							partialMsg.setDurableProperties(stringToByteBuf(dataObj.getAttributes()));
							partialMsg.setBody(Unpooled.wrappedBuffer(dataObj.getPayload()));
							partialMsg.setBornTime(dataObj.getCreationDate().getTime());
							partialMsg.setKey(dataObj.getRefKey());

							codec.encode(partialMsg, out);
						}
					}

					// TODO move to util
					private ByteBuf stringToByteBuf(String str) {
						return Unpooled.wrappedBuffer(str.getBytes(Charsets.ISO_8859_1));
					}
				});

				batch.setResend(false);
				result.setBatch(batch);
				result.setOffset(biggestOffset);
				return result;
			}
		} catch (Exception e) {
			e.printStackTrace();
			// TODO
		}

		return null;
	}

	@Override
	public void nack(Tpp tpp, String groupId, boolean resend, List<Pair<Long, Integer>> msgSeqs) {
		if (CollectionUtil.isNotEmpty(msgSeqs)) {
			// TODO retry policy should support per consumer?
			RetryPolicy retryPolicy = m_metaService.findTopic(tpp.getTopic()).getRetryPolicy();
			int initRemaingRetries = retryPolicy.getRetryTimes();

			List<Pair<Long, Integer>> toDeadLetter = new ArrayList<>();
			List<Pair<Long, Integer>> toResend = new ArrayList<>();
			for (Pair<Long, Integer> pair : msgSeqs) {
				if (resend) {
					pair.setValue(pair.getValue() - 1);
				} else {
					pair.setValue(initRemaingRetries);
				}

				if (pair.getValue() <= 0) {
					toDeadLetter.add(pair);
				} else {
					toResend.add(pair);
				}

			}

			try {
				copyToDeadLetter(tpp, groupId, toDeadLetter, resend);
				copyToResend(tpp, groupId, toResend, resend, initRemaingRetries);
			} catch (DalException e) {
				// TODO
				e.printStackTrace();
			}
		}
	}

	private void copyToResend(Tpp tpp, String groupId, List<Pair<Long, Integer>> msgSeqs, boolean resend,
	      int initRemaingRetries) throws DalException {
		if (CollectionUtil.isNotEmpty(msgSeqs)) {
			ResendGroupId proto = new ResendGroupId();
			proto.setTopic(tpp.getTopic());
			proto.setPartition(tpp.getPartition());
			proto.setPriority(tpp.getPriorityInt());
			proto.setGroupId(m_metaService.getGroupIdInt(groupId));
			// TODO schedule date and remaining retry should base on how much times has the message be resent
			proto.setScheduleDate(new Date(System.currentTimeMillis() + 5000));
			proto.setMessageIds(collectOffset(msgSeqs));

			if (resend) {
				m_resendDao.copyFromResendTable(proto);
			} else {
				proto.setRemainingRetries(initRemaingRetries);
				m_resendDao.copyFromMessageTable(proto);
			}
		}
	}

	private void copyToDeadLetter(Tpp tpp, String groupId, List<Pair<Long, Integer>> msgSeqs, boolean resend)
	      throws DalException {
		if (CollectionUtil.isNotEmpty(msgSeqs)) {
			DeadLetter proto = new DeadLetter();
			proto.setTopic(tpp.getTopic());
			proto.setPartition(tpp.getPartition());
			proto.setPriority(tpp.getPriorityInt());
			proto.setGroupId(m_metaService.getGroupIdInt(groupId));
			proto.setDeadDate(new Date());
			proto.setMessageIds(collectOffset(msgSeqs));

			if (resend) {
				m_deadLetterDao.copyFromResendTable(proto);
			} else {
				m_deadLetterDao.copyFromMessageTable(proto);
			}
		}
	}

	private Long[] collectOffset(List<Pair<Long, Integer>> msgSeqs) {
		Long[] offsets = new Long[msgSeqs.size()];

		int idx = 0;
		for (Pair<Long, Integer> pair : msgSeqs) {
			offsets[idx++] = pair.getKey();
		}

		return offsets;
	}

	@Override
	public void ack(Tpp tpp, String groupId, boolean resend, long msgSeq) {
		try {
			String topic = tpp.getTopic();
			int partition = tpp.getPartition();
			int intGroupId = m_metaService.getGroupIdInt(groupId);
			if (resend) {
				// TODO cache OffsetResend's id
				ResendGroupId resendRow = m_resendDao.findByPK(msgSeq, topic, partition, intGroupId,
				      ResendGroupIdEntity.READSET_FULL);
				List<OffsetResend> offsetResendRow = m_offsetResendDao.top(topic, partition, intGroupId,
				      OffsetResendEntity.READSET_FULL);

				// TODO ensure offsetResendRow is inserted by findLastResendOffset
				OffsetResend proto = CollectionUtil.first(offsetResendRow);
				proto.setTopic(topic);
				proto.setPartition(partition);
				proto.setLastScheduleDate(resendRow.getScheduleDate());
				proto.setLastId(resendRow.getId());

				m_offsetResendDao.updateByPK(proto, OffsetResendEntity.UPDATESET_OFFSET);
			} else {
				// TODO cache OffsetMessage's id
				List<OffsetMessage> offsetMessageRow = m_offsetMessageDao.find(topic, partition, tpp.getPriorityInt(),
				      intGroupId, OffsetMessageEntity.READSET_FULL);

				// TODO ensure offsetMessageRow is inserted by findLastOffset
				OffsetMessage proto = CollectionUtil.first(offsetMessageRow);
				proto.setTopic(topic);
				proto.setPartition(partition);
				proto.setOffset(msgSeq);

				m_offsetMessageDao.updateByPK(proto, OffsetMessageEntity.UPDATESET_OFFSET);
			}
		} catch (DalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public synchronized Object findLastResendOffset(Tpg tpg) throws Exception {
		int groupId = m_metaService.getGroupIdInt(tpg.getGroupId());
		List<OffsetResend> tops = m_offsetResendDao.top(tpg.getTopic(), tpg.getPartition(), groupId,
		      OffsetResendEntity.READSET_FULL);
		if (CollectionUtil.isNotEmpty(tops)) {
			OffsetResend top = CollectionUtil.first(tops);
			return new Pair<>(top.getLastScheduleDate(), top.getLastId());
		} else {
			OffsetResend proto = new OffsetResend();
			proto.setTopic(tpg.getTopic());
			proto.setPartition(tpg.getPartition());
			proto.setGroupId(m_metaService.getGroupIdInt(tpg.getGroupId()));
			proto.setLastScheduleDate(new Date(0));
			proto.setLastId(0L);
			proto.setCreationDate(new Date());

			m_offsetResendDao.insert(proto);
			return new Pair<>(proto.getLastScheduleDate(), proto.getLastId());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public FetchResult fetchResendMessages(Tpg tpg, Object startOffset, int batchSize) {
		Pair<Date, Long> startPair = (Pair<Date, Long>) startOffset;
		FetchResult result = new FetchResult();

		try {
			final List<ResendGroupId> dataObjs = m_resendDao.find(tpg.getTopic(), tpg.getPartition(),
			      m_metaService.getGroupIdInt(tpg.getGroupId()), startPair.getKey(), batchSize, startPair.getValue(),
			      new Date(), ResendGroupIdEntity.READSET_FULL);

			if (CollectionUtil.isNotEmpty(dataObjs)) {
				TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
				ResendGroupId latestResend = new ResendGroupId();
				latestResend.setScheduleDate(new Date(0));
				latestResend.setId(0L);

				for (ResendGroupId dataObj : dataObjs) {
					if (resendAfter(dataObj, latestResend)) {
						latestResend = dataObj;
					}

					batch.addMsgSeq(dataObj.getId(), dataObj.getRemainingRetries());
				}
				final String topic = tpg.getTopic();
				batch.setTopic(topic);
				batch.setPartition(tpg.getPartition());

				batch.setTransferCallback(new TransferCallback() {

					@Override
					public void transfer(ByteBuf out) {
						for (ResendGroupId dataObj : dataObjs) {
							MessageCodec codec = MessageCodecFactory.getCodec(topic);
							PartialDecodedMessage partialMsg = new PartialDecodedMessage();
							partialMsg.setRemainingRetries(dataObj.getRemainingRetries());
							partialMsg.setDurableProperties(stringToByteBuf(dataObj.getAttributes()));
							partialMsg.setBody(Unpooled.wrappedBuffer(dataObj.getPayload()));
							partialMsg.setBornTime(dataObj.getCreationDate().getTime());
							partialMsg.setKey(dataObj.getRefKey());

							codec.encode(partialMsg, out);
						}
					}

					// TODO move to util
					private ByteBuf stringToByteBuf(String str) {
						return Unpooled.wrappedBuffer(str.getBytes(Charsets.ISO_8859_1));
					}
				});

				batch.setResend(true);
				result.setBatch(batch);
				result.setOffset(new Pair<Date, Long>(latestResend.getScheduleDate(), latestResend.getId()));
			}
			return result;
		} catch (DalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	private boolean resendAfter(ResendGroupId l, ResendGroupId r) {
		if (l.getScheduleDate().after(r.getScheduleDate())) {
			return true;
		}
		if (l.getScheduleDate().equals(r.getScheduleDate()) && l.getId() > r.getId()) {
			return true;
		}

		return false;
	}

}
