package com.ctrip.hermes.storage.storage.memory;

import java.util.Map;

import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.spi.typed.MessageStorage;

public class MemoryMessageStorage extends AbstractMemoryStorage<Message> implements MessageStorage{

	public MemoryMessageStorage(String id) {
		super(id);
	}

	@Override
	protected Message clone(Message msg) {
		Message newMsg = new Message();

		newMsg.setAckOffset(msg.getAckOffset());
		newMsg.setContent(msg.getContent());
		newMsg.setOffset(msg.getOffset());
		newMsg.setPriority(msg.getPriority());
		newMsg.setPartition(msg.getPartition());

		for (Map.Entry<String, String> entry : newMsg.getProperties().entrySet()) {
			newMsg.setProperty(entry.getKey(), entry.getValue());
		}

		return newMsg;
	}

}
