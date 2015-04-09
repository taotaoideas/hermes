package com.ctrip.hermes.broker.queue.mysql;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.unidal.dal.jdbc.DalException;

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

	protected void doAppendMessageSync(MessageRawDataBatch batch, boolean isPriority, Map<Integer, Boolean> result) {

		List<PartialDecodedMessage> pdmsgs = batch.getMessages();
		List<MessagePriority> msgs = new ArrayList<>(pdmsgs.size());
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

		try {
			m_messageService.write(msgs);

			addResults(result, true);
		} catch (DalException e) {
			addResults(result, false);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
