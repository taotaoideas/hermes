package com.ctrip.hermes.broker.remoting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.channel.MessageQueueManager;
import com.ctrip.hermes.core.message.DecodedProducerMessage;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.SendMessageAckCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.Tpp;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.storage.MessageQueue;
import com.ctrip.hermes.storage.message.Record;

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
			Tpp tpp = entry.getKey();
			MessageQueue q = m_queueManager.findQueue(tpp);
			q.write(convertToRecord(entry.getValue().getMessages(), tpp));
		}

		SendMessageAckCommand ack = new SendMessageAckCommand();
		ack.correlate(req);

		ctx.write(ack);

	}

	private List<Record> convertToRecord(List<DecodedProducerMessage> messages, Tpp tpp) {
		List<Record> records = new ArrayList<>();

		for (DecodedProducerMessage msg : messages) {
			Record r = new Record();

			r.setBornTime(msg.getBornTime());
			r.setContent(msg.readBody());
			r.setKey(msg.getKey());
			// TODO should add other fields after refactoring MessageQueue

			records.add(r);
		}

		return records;
	}

}
