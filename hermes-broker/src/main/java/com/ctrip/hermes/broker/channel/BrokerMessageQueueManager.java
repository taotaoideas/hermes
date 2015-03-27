package com.ctrip.hermes.broker.channel;

import java.util.HashMap;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.Tpp;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.storage.MessageQueue;
import com.ctrip.hermes.storage.impl.StorageMessageQueue;
import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.pair.StoragePair;
import com.ctrip.hermes.storage.storage.memory.MemoryGroup;
import com.ctrip.hermes.storage.storage.memory.MemoryGroupConfig;
import com.ctrip.hermes.storage.storage.memory.MemoryStorageFactory;

public class BrokerMessageQueueManager implements MessageQueueManager {

	public final static String ID = "broker";

	@Inject
	private MetaService m_meta;

	private Map<Pair<String, String>, MessageQueue> m_queues = new HashMap<Pair<String, String>, MessageQueue>();

	private MemoryStorageFactory storageFactory = new MemoryStorageFactory();

	@Override
	public MessageQueue findQueue(Tpp tpp) {
		Storage storage = m_meta.findStorage(tpp.getTopic());
		if (storage == null) {
			throw new RuntimeException("Undefined topic: " + tpp.getTopic());
		}
		/**
		 * if can't find following constance, try run "mvn generate-sources" in command line.
		 */
		if (Storage.MEMORY.equals(storage.getType())) {
			return findMemoryQueue(tpp);
		} else {
			// TODO
			throw new RuntimeException("Unsupported storage type");
		}
	}

	private synchronized MessageQueue findMemoryQueue(Tpp tpp) {
		String strTpp = tpp.getTopic() + "_" + tpp.getPartitionNo() + "_" + tpp.isPriority();
		String groupId = "for_producer";

		Pair<String, String> pair = new Pair<String, String>(strTpp, groupId);

		MessageQueue q = m_queues.get(pair);

		if (q == null) {
			MemoryGroupConfig gc = new MemoryGroupConfig();
			gc.addMainGroup(strTpp, "offset_" + strTpp + "_" + groupId);
			gc.setResendGroupId("resend_" + strTpp + "_" + groupId, "offset_resend_" + strTpp + "_" + groupId);
			MemoryGroup mg = new MemoryGroup(storageFactory, gc);

			StoragePair<Record> main = mg.createMessagePair();
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
