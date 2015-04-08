package com.ctrip.hermes.broker.queue;

import java.util.Map;

import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueue {

	ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(MessageRawDataBatch batch, boolean isPriority);

	MessageQueueCursor createCursor(String groupId);

}
