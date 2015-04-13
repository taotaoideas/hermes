package com.ctrip.hermes.broker.queue.mysql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.service.MessageService;
import com.ctrip.hermes.broker.queue.AbstractMessageQueueDumper;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.google.common.base.Charsets;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class MySQLMessageQueueDumper extends AbstractMessageQueueDumper {

	private MessageService m_messageService;

	public MySQLMessageQueueDumper(String topic, int partition) {
		super(topic, partition);
		m_messageService = PlexusComponentLocator.lookup(MessageService.class);
	}

	@Override
	protected void doAppendMessageSync(Collection<Pair<MessageRawDataBatch, Map<Integer, Boolean>>> todos, boolean isPriority) {
		List<MessagePriority> msgs = new ArrayList<>();
		for (Pair<MessageRawDataBatch, Map<Integer, Boolean>> todo : todos) {
			List<PartialDecodedMessage> pdmsgs = todo.getKey().getMessages();
			for (PartialDecodedMessage pdmsg : pdmsgs) {
				MessagePriority msg = new MessagePriority();
				msg.setAttributes(new String(pdmsg.readAppProperties(), Charsets.UTF_8));
				msg.setCreationDate(new Date(pdmsg.getBornTime()));
				msg.setPartition(m_partition);
				msg.setPayload(pdmsg.readBody());
				// TODO
				msg.setPriority(isPriority ? 0 : 1);
				// TODO set producer id and producer id in producer
				msg.setProducerId(101);
				msg.setProducerIp("1.1.1.1");
				msg.setRefKey(pdmsg.getKey());
				msg.setTopic(m_topic);

				msgs.add(msg);
			}
		}

		try {
			m_messageService.write(msgs);

			for (Pair<MessageRawDataBatch, Map<Integer, Boolean>> todo : todos) {
				Map<Integer, Boolean> result = todo.getValue();
				addResults(result, true);
			}
		} catch (DalException e) {
			for (Pair<MessageRawDataBatch, Map<Integer, Boolean>> todo : todos) {
				Map<Integer, Boolean> result = todo.getValue();
				addResults(result, false);
			}
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
