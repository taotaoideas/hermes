package com.ctrip.hermes.consumer.engine.command.processor;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.ConsumeMessageCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.processor.SingleThreaded;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@SingleThreaded
public class ConsumeMessageCommandProcessor implements CommandProcessor {

	@Inject
	private MessageCodec m_messageCodec;

	@Inject
	private ConsumerNotifier m_consumerNotifier;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.core.transport.command.processor.CommandProcessor#commandTypes()
	 */
	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_CONSUME);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		Command cmd = ctx.getCommand();
		if (cmd instanceof ConsumeMessageCommand) {
			ConsumeMessageCommand consumeMessageCommand = (ConsumeMessageCommand) cmd;

			try {
				for (Map.Entry<Long, List<TppConsumerMessageBatch>> entry : consumeMessageCommand.getMsgs().entrySet()) {
					long correlationId = entry.getKey();
					List<TppConsumerMessageBatch> batches = entry.getValue();

					Class<?> bodyClazz = m_consumerNotifier.find(correlationId).getMessageClazz();

					List<ConsumerMessage<?>> msgs = decodeBatches(batches, bodyClazz, ctx.getChannel());

					m_consumerNotifier.messageReceived(correlationId, msgs);
				}
			} finally {
				consumeMessageCommand.release();
			}
		}

	}

	@SuppressWarnings("rawtypes")
	private List<ConsumerMessage<?>> decodeBatches(List<TppConsumerMessageBatch> batches, Class bodyClazz,
	      EndpointChannel channel) {
		List<ConsumerMessage<?>> msgs = new ArrayList<>();
		for (TppConsumerMessageBatch batch : batches) {
			List<Pair<Long, Integer>> msgSeqs = batch.getMsgSeqs();
			ByteBuf batchData = batch.getData();

			int partition = batch.getPartition();
			boolean priority = batch.isPriority();

			for (int j = 0; j < msgSeqs.size(); j++) {
				BaseConsumerMessage baseMsg = m_messageCodec.decode(batch.getTopic(), batchData, bodyClazz);
				BrokerConsumerMessage brokerMsg = new BrokerConsumerMessage(baseMsg);
				brokerMsg.setPartition(partition);
				brokerMsg.setPriority(priority);
				brokerMsg.setResend(batch.isResend());
				brokerMsg.setChannel(channel);
				brokerMsg.setMsgSeq(msgSeqs.get(j).getKey());

				msgs.add(brokerMsg);
			}
		}

		return msgs;
	}
}
