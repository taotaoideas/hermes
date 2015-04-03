package com.ctrip.hermes.broker.queue;

import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.dal.hermes.MTopicPartitionPriority;
import com.ctrip.hermes.broker.dal.hermes.MTopicPartitionPriorityDao;
import com.ctrip.hermes.broker.dal.hermes.MTopicPartitionPriorityEntity;

public class MysqlQueueReader implements QueueReader {

	@Inject
	private MTopicPartitionPriorityDao m_msgDao;

	private String m_topic;

	private int m_shard;

	@Override
	public List<MTopicPartitionPriority> read(int priority, long startId, int batchSize) throws DalException {
		return m_msgDao.findIdAfter(m_topic, m_shard, priority, startId, batchSize, MTopicPartitionPriorityEntity.READSET_FULL);
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public void setShard(int shard) {
		m_shard = shard;
	}

}
