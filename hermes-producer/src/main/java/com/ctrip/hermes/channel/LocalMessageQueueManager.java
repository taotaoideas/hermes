package com.ctrip.hermes.channel;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;
import kafka.serializer.StringEncoder;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.storage.MessageQueue;
import com.ctrip.hermes.storage.impl.StorageMessageQueue;
import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.pair.StoragePair;
import com.ctrip.hermes.storage.storage.kafka.KafkaGroup;
import com.ctrip.hermes.storage.storage.memory.MemoryGroup;
import com.ctrip.hermes.storage.storage.memory.MemoryGroupConfig;
import com.ctrip.hermes.storage.storage.memory.MemoryStorageFactory;
import com.ctrip.hermes.storage.storage.mysql.MysqlGroup;

public class LocalMessageQueueManager implements MessageQueueManager {

	@Inject
	private MetaService m_meta;

	private Map<Pair<String, String>, MessageQueue> m_queues = new HashMap<Pair<String, String>, MessageQueue>();

	private MemoryStorageFactory storageFactory = new MemoryStorageFactory();

	@Override
	public MessageQueue findQueue(String topic, String groupId, String partition) {
		if (partition == null) {
			partition = "invalid";
		}
		Storage storage = m_meta.getStorage(topic);
		if (storage == null) {
			throw new RuntimeException("Undefined topic: " + topic);
		}
		/**
		 * if can't find following constance, try run "mvn generate-sources" in command line.
		 */
		if (Storage.MEMORY.equals(storage.getType())) {
			return findMemoryQueue(topic, groupId);
		} else if (Storage.KAFKA.equals(storage.getType())) {
			return findKafkaQueue(topic, groupId, partition);
		} else if (Storage.MYSQL.equals(storage.getType())) {
			return findMySQLQueue(topic, groupId);
		} else {
			// TODO
			throw new RuntimeException("Unsupported storage type");
		}
	}

	@Override
	public MessageQueue findQueue(String topic, String groupId) {
		return findQueue(topic, groupId, "invalid");
	}

	@Override
	public MessageQueue findQueue(String topic) {
		return findQueue(topic, "invalid");
	}

	private synchronized MessageQueue findMemoryQueue(String topic, String groupId) {
		Pair<String, String> pair = new Pair<String, String>(topic, groupId);

		MessageQueue q = m_queues.get(pair);

		if (q == null) {
			MemoryGroupConfig gc = new MemoryGroupConfig();
			gc.addMainGroup(topic, "offset_" + topic + "_" + groupId);
			gc.setResendGroupId("resend_" + topic + "_" + groupId, "offset_resend_" + topic + "_" + groupId);
			MemoryGroup mg = new MemoryGroup(storageFactory, gc);

			StoragePair<Message> main = mg.createMessagePair();
			StoragePair<Resend> resend = mg.createResendPair();
			q = new StorageMessageQueue(main, resend);

			m_queues.put(pair, q);
		}

		return q;

	}

	private synchronized MessageQueue findKafkaQueue(String topic, String groupId, String partition) {
		Pair<String, String> pair = new Pair<String, String>(topic, groupId);

		MessageQueue q = m_queues.get(pair);

		if (q == null) {
			Properties props = new Properties();
			Storage storage = m_meta.getStorage(topic);
			for (Property prop : storage.getProperties()) {
				props.put(prop.getName(), prop.getValue());
			}
			props.put("serializer.class", DefaultEncoder.class.getCanonicalName());
			props.put("key.serializer.class", StringEncoder.class.getCanonicalName());
			props.put("group.id", groupId);
			ProducerConfig pc = new ProducerConfig(props);
			ConsumerConfig cc = new ConsumerConfig(props);

			KafkaGroup kg = new KafkaGroup(topic, groupId, partition, pc, cc);

			StoragePair<Message> main = kg.createMessagePair();
			StoragePair<Resend> resend = kg.createResendPair();
			q = new StorageMessageQueue(main, resend);

			m_queues.put(pair, q);
		}

		return q;
	}

	private synchronized MessageQueue findMySQLQueue(String topic, String groupId) {
		Pair<String, String> pair = new Pair<String, String>(topic, groupId);

		MessageQueue q = m_queues.get(pair);

		if (q == null) {
			MysqlGroup mg = new MysqlGroup(groupId);

			StoragePair<Message> main = mg.createMessagePair();
			StoragePair<Resend> resend = mg.createResendPair();
			q = new StorageMessageQueue(main, resend);

			m_queues.put(pair, q);
		}

		return q;
	}

	public Map<Pair<String, String>, MessageQueue> getQueues() {
		return m_queues;
	}

}
