package com.ctrip.hermes.broker.queue;

import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.dal.hermes.MTopicShardPriority;
import com.ctrip.hermes.broker.dal.hermes.MTopicShardPriorityDao;

public class MysqlQueueReader implements QueueReader {

	@Inject
	private MTopicShardPriorityDao m_msgDao;

	private String m_topic;

	private int m_shard;

	@Override
	public List<MTopicShardPriority> read(long startId, int batchSize) {
		// m_msgDao.find(m_topic, m_shard, 0);
		return null;
	}

}
