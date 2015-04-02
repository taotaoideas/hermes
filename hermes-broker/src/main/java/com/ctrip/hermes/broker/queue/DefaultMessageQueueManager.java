package com.ctrip.hermes.broker.queue;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.Tpp;
import com.ctrip.hermes.meta.entity.Storage;

public class DefaultMessageQueueManager extends ContainerHolder implements MessageQueueManager {

	public final static String ID = "broker";

	@Inject
	private MetaService m_meta;

	@Override
	public void write(Tpp tpp, MessageRawDataBatch data) throws StorageException {
		Storage storage = m_meta.findStorage(tpp.getTopic());
		if (storage == null) {
			throw new RuntimeException("Undefined topic: " + tpp.getTopic());
		}

		// TODO support other storage
		if (Storage.MYSQL.equals(storage.getType())) {
			QueueWriter writer = lookup(QueueWriter.class, Storage.MYSQL);
			writer.write(tpp, data);
		} else {
			// TODO
			throw new RuntimeException("Unsupported storage type " + storage.getType());
		}

	}

	@Override
	public QueueReader createReader(String topic, int shard) {
		Storage storage = m_meta.findStorage(topic);
		if (storage == null) {
			throw new RuntimeException("Undefined topic: " + topic);
		}

		// TODO support other storage
		if (Storage.MYSQL.equals(storage.getType())) {
			MysqlQueueReader reader = (MysqlQueueReader) lookup(QueueReader.class, Storage.MYSQL);
			reader.setTopic(topic);
			reader.setShard(shard);
			
			return reader;
		} else {
			// TODO
			throw new RuntimeException("Unsupported storage type " + storage.getType());
		}
	}

}
