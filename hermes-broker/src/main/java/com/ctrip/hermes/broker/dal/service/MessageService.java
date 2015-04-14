package com.ctrip.hermes.broker.dal.service;

import java.util.Date;
import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.dal.hermes.DeadLetter;
import com.ctrip.hermes.broker.dal.hermes.DeadLetterDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityEntity;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessage;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessageDao;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessageEntity;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.utils.CollectionUtil;

@Named(type = MessageService.class)
public class MessageService {

	@Inject
	private MessagePriorityDao m_msgDao;

	@Inject
	private OffsetMessageDao m_offsetDao;

	@Inject
	private DeadLetterDao m_deadLetterDao;

	public void write(List<MessagePriority> msgs) throws DalException {

		m_msgDao.insert(msgs.toArray(new MessagePriority[msgs.size()]));
	}

	public OffsetMessage findLastOffset(Tpp tpp, int groupId) throws DalException {
		String topic = tpp.getTopic();
		int partition = tpp.getPartition();
		int priority = tpp.getPriorityInt();

		List<OffsetMessage> lastOffset = m_offsetDao.find(topic, partition, priority, groupId,
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

			m_offsetDao.insert(offset);
			return offset;
		} else {
			return CollectionUtil.last(lastOffset);
		}
	}

	public List<MessagePriority> read(Tpp tpp, long startId, int batchSize) throws DalException {
		return m_msgDao.findIdAfter(tpp.getTopic(), tpp.getPartition(), tpp.getPriorityInt(), startId, batchSize,
		      MessagePriorityEntity.READSET_FULL);
	}

	public void updateOffset(OffsetMessage offset, long newOffset) throws DalException {
		offset.setOffset(newOffset);
		m_offsetDao.updateByPK(offset, OffsetMessageEntity.UPDATESET_OFFSET);
	}

	public void deadLetter(DeadLetter dl) throws DalException {
		m_deadLetterDao.insert(dl);
	}
}
