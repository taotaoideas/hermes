package com.ctrip.hermes.broker.queue;

import java.util.List;

import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.broker.dal.hermes.MTopicPartitionPriority;

public interface QueueReader {

	List<MTopicPartitionPriority> read(int priority, long startId, int batchSize) throws DalException;

}
