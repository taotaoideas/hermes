package com.ctrip.hermes.broker.queue.storage;

import java.util.Collection;
import java.util.List;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueueStorage {

	void appendMessages(Tpp tpp, Collection<MessageRawDataBatch> batches) throws Exception;

	long findLastOffset(Tpp tpp, int groupId) throws Exception;

	long findLastResendOffset(Tpg tpg) throws Exception;

	TppConsumerMessageBatch fetchMessages(Tpp tpp, long startOffset, int batchSize);

	TppConsumerMessageBatch fetchResendMessages(Tpg tpg, long startOffset, int batchSize);

	void nack(Tpp tpp, String groupId, boolean resend, List<Long> msgSeqs);

	void ack(Tpp tpp, String groupId, boolean resend, long msgSeq);

}
