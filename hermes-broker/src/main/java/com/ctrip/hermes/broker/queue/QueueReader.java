package com.ctrip.hermes.broker.queue;

import java.util.List;

import com.ctrip.hermes.broker.dal.hermes.MTopicShardPriority;

public interface QueueReader {

	List<MTopicShardPriority> read(long startId, int batchSize);

}
