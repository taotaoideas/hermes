package com.ctrip.hermes.broker.remoting;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.channel.MessageQueueManager;
import com.ctrip.hermes.broker.dal.hermes.MTopicShardPriority;
import com.ctrip.hermes.broker.dal.hermes.MTopicShardPriorityDao;
import com.ctrip.hermes.core.message.DecodedProducerMessage;
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

	@Inject
	private MTopicShardPriorityDao m_dao;

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
			try {
				saveToMysql(entry.getValue().getMessages(), tpp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		SendMessageAckCommand ack = new SendMessageAckCommand();
		ack.correlate(req);

		ctx.write(ack);

	}

	private void saveToMysql(List<DecodedProducerMessage> messages, Tpp tpp) throws Exception {
		for (DecodedProducerMessage msg : messages) {
			MTopicShardPriority r = new MTopicShardPriority();

			r.setCreationDate(new Date(msg.getBornTime()));
			r.setPayload(msg.readBody());
			r.setProducerId(1);
			r.setProducerIp("1.1.1.1");
			r.setRefKey(msg.getKey());

			r.setTopic(tpp.getTopic());
			r.setShard(tpp.getPartitionNo());
			r.setPriority(tpp.isPriority() ? 0 : 1);

			m_dao.insert(r);
		}
	}

}
