package com.ctrip.hermes.broker.queue.partition;

import java.util.Arrays;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Storage;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MessageQueuePartitionFactory.class)
public class MessageQueuePartitionFactory extends ContainerHolder {
	@Inject
	private MetaService m_metaService;

	public MessageQueuePartition createMessageQueuePartition(String topic, int partition) {
		Storage storage = m_metaService.findStorage(topic);

		if (Arrays.asList(Storage.MYSQL).contains(storage.getType())) {
			return new DefaultMessageQueuePartition(topic, partition,
			      lookup(MessageQueueStorage.class, storage.getType()), m_metaService);
		} else {
			throw new IllegalArgumentException("Unsupported storage type " + storage.getType());
		}
	}
}
