package com.ctrip.hermes.broker.remoting;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.broker.queue.StorageException;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.SendMessageAckCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.Tpp;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;

public class SendMessageRequestProcessor implements CommandProcessor {

	public static final String ID = "send-message-request";

	@Inject
	private MessageQueueManager m_queueManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_SEND);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		SendMessageCommand req = (SendMessageCommand) ctx.getCommand();

		Map<Tpp, MessageRawDataBatch> rawBatches = req.getMessageRawDataBatches();

		for (Map.Entry<Tpp, MessageRawDataBatch> entry : rawBatches.entrySet()) {
			try {
				m_queueManager.write(entry.getKey(), entry.getValue());
			} catch (StorageException e) {
				// TODO
				e.printStackTrace();
			}
		}

		SendMessageAckCommand ack = new SendMessageAckCommand();
		ack.correlate(req);

		ctx.write(ack);

	}

}
