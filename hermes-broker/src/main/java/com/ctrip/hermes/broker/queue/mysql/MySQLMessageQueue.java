package com.ctrip.hermes.broker.queue.mysql;

import com.ctrip.hermes.broker.queue.AbstractMessageQueue;
import com.ctrip.hermes.broker.queue.MessageQueueCursor;
import com.ctrip.hermes.broker.queue.MessageQueueDumper;
import com.ctrip.hermes.core.bo.Tpg;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class MySQLMessageQueue extends AbstractMessageQueue {

	public MySQLMessageQueue(String topic, int partition) {
		super(topic, partition);

	}

	@Override
	protected MessageQueueDumper getMessageQueueDumper(String topic, int partition) {
		return new MySQLMessageQueueDumper(topic, partition);
	}

	@Override
	protected MessageQueueCursor doCreateCursor(String groupId) {
		return new MySQLMessageQueueCursor(new Tpg(m_topic, m_partition, groupId));
	}

}
