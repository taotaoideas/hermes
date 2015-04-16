package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.partition.MessageQueuePartitionCursor;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.google.common.util.concurrent.ListenableFuture;

public interface MessageQueueManager {

	public ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(Tpp tpp, MessageRawDataBatch data);

	public MessageQueuePartitionCursor createCursor(Tpg tpg);

	public void nack(Tpp tpp, String groupId, boolean resend, List<Pair<Long, Integer>> msgSeqs);

	public void ack(Tpp tpp, String groupId, boolean resend, long msgSeq);
}
