package com.ctrip.hermes.broker.queue;

import java.util.Map;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.google.common.util.concurrent.ListenableFuture;

public interface MessageQueueManager {

	public ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(Tpp tpp, MessageRawDataBatch data);

	public MessageQueueCursor createCursor(Tpg tpg);

}
