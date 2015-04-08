package com.ctrip.hermes.broker.queue;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Storage;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class MessageQueueFactory {
	public static MessageQueue createMessageQueue(String topic, int partition) {
		Storage storage = PlexusComponentLocator.lookup(MetaService.class).findStorage(topic);

		switch (storage.getType()) {
		case Storage.MYSQL:
			MySQLMessageQueue mq = new MySQLMessageQueue(topic, partition);
			return mq;

		default:
			throw new IllegalArgumentException("Unsupported storage type " + storage.getType());
		}
	}
}
