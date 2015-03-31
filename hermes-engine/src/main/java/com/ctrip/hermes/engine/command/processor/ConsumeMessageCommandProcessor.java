package com.ctrip.hermes.engine.command.processor;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessageBatch;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.message.codec.MessageCodecFactory;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.ConsumeMessageCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.engine.notifier.ConsumerNotifier;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ConsumeMessageCommandProcessor implements CommandProcessor {

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

			for (Map.Entry<Long, List<ConsumerMessageBatch>> entry : consumeMessageCommand.getMsgs().entrySet()) {
				long correlationId = entry.getKey();
				List<ConsumerMessageBatch> batches = entry.getValue();

				Class<?> bodyClazz = m_consumerNotifier.find(correlationId).getMessageClazz();

				List<ConsumerMessage<?>> msgs = decodeBatches(batches, bodyClazz);

				m_consumerNotifier.messageReceived(correlationId, msgs);
			}
		}

	}

	@SuppressWarnings("rawtypes")
   private List<ConsumerMessage<?>> decodeBatches(List<ConsumerMessageBatch> batches, Class bodyClazz) {
		List<ConsumerMessage<?>> msgs = new ArrayList<>();
		for (ConsumerMessageBatch batch : batches) {
			List<Long> msgSeqs = batch.getMsgSeqs();
			ByteBuf batchData = batch.getData();

			MessageCodec codec = MessageCodecFactory.getCodec(batch.getTopic());

			for (int j = 0; j < msgSeqs.size(); j++) {
				BaseConsumerMessage baseMsg = codec.decode(batchData, bodyClazz);
				BrokerConsumerMessage brokerMsg = new BrokerConsumerMessage(baseMsg);
				brokerMsg.setMsgSeq(msgSeqs.get(j));

				msgs.add(brokerMsg);
			}
		}

		return msgs;
	}
}
