package com.ctrip.hermes.broker.queue;

import java.util.List;

import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;

public interface QueueReader {

	List<MessagePriority> read(int priority, long startId, int batchSize) throws DalException;

}
