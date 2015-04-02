package com.ctrip.hermes.broker.queue;

import java.util.List;

import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.broker.dal.hermes.MTopicShardPriority;

public interface QueueReader {

	List<MTopicShardPriority> read(int priority, long startId) throws DalException;

}
