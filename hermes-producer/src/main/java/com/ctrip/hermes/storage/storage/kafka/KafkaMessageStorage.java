package com.ctrip.hermes.storage.storage.kafka;

import java.util.List;

import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.spi.typed.MessageStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;

public class KafkaMessageStorage implements MessageStorage {

	@Override
	public void append(List<Message> payloads) throws StorageException {
		// TODO Auto-generated method stub

	}

	@Override
	public Browser<Message> createBrowser(long offset) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Message> read(Range range) throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message top() throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		// TODO Auto-generated method stub
		return null;
	}

}
