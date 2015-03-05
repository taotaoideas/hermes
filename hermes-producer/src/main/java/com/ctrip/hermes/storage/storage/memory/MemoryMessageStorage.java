package com.ctrip.hermes.storage.storage.memory;

import java.util.Map;

import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.spi.typed.MessageStorage;

public class MemoryMessageStorage extends AbstractMemoryStorage<Record> implements MessageStorage {

	public MemoryMessageStorage(String id) {
		super(id);
	}

	@Override
	protected Record clone(Record msg) {
		Record newMsg = new Record();

		newMsg.setAckOffset(msg.getAckOffset());
		newMsg.setContent(msg.getContent());
		newMsg.setOffset(msg.getOffset());
		newMsg.setPriority(msg.getPriority());
		newMsg.setPartition(msg.getPartition());
		newMsg.setKey(msg.getKey());
		newMsg.setBornTime(msg.getBornTime());
		newMsg.setProperties(msg.getProperties());

		for (Map.Entry<String, Object> entry : newMsg.getProperties().entrySet()) {
			newMsg.setProperty(entry.getKey(), entry.getValue());
		}

		return newMsg;
	}

}
