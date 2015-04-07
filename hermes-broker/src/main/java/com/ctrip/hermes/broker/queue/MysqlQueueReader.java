package com.ctrip.hermes.broker.queue;

import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityEntity;

public class MysqlQueueReader implements QueueReader {

	@Inject
	private MessagePriorityDao m_msgDao;

	private String m_topic;

	private int m_shard;

	@Override
	public List<MessagePriority> read(int priority, long startId, int batchSize) throws DalException {
		return m_msgDao.findIdAfter(m_topic, m_shard, priority, startId, batchSize, MessagePriorityEntity.READSET_FULL);
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public void setShard(int shard) {
		m_shard = shard;
	}

}
