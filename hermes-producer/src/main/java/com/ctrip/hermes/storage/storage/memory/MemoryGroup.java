package com.ctrip.hermes.storage.storage.memory;

import java.util.ArrayList;
import java.util.List;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.pair.ClusteredMessagePair;
import com.ctrip.hermes.storage.pair.MessagePair;
import com.ctrip.hermes.storage.pair.ResendPair;
import com.ctrip.hermes.storage.storage.Offset;

public class MemoryGroup {

	private MemoryStorageFactory m_storageFactory;

	private MemoryGroupConfig m_groupConfig;

	public MemoryGroup(MemoryStorageFactory storageFactory, MemoryGroupConfig groupConfig) {
		m_storageFactory = storageFactory;
		m_groupConfig = groupConfig;
	}

	public ClusteredMessagePair createMessagePair() {
		List<MessagePair> pairs = new ArrayList<MessagePair>();

		List<Pair<String, String>> mainIdPairs = m_groupConfig.mainGroupIds();

		for (Pair<String, String> idPair : mainIdPairs) {
			AbstractMemoryStorage<Record> m = m_storageFactory.findMessageStorage(idPair.getKey());
			AbstractMemoryStorage<Offset> o = m_storageFactory.findOffsetStorage(idPair.getValue());
			pairs.add(new MessagePair(m, o));
		}

		return new ClusteredMessagePair(pairs);
	}

	public ResendPair createResendPair() {
		Pair<String, String> resendIdPair = m_groupConfig.resendGroupId();

		AbstractMemoryStorage<Resend> m = m_storageFactory.findResendStorage(resendIdPair.getKey());
		AbstractMemoryStorage<Offset> o = m_storageFactory.findOffsetStorage(resendIdPair.getValue());

		ResendPair pair = new ResendPair(m, o, Long.MAX_VALUE);

		return pair;
	}

}
