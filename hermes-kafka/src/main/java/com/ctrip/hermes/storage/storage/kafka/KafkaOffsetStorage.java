package com.ctrip.hermes.storage.storage.kafka;

import java.util.List;

import com.ctrip.hermes.storage.spi.typed.OffsetStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;

public class KafkaOffsetStorage implements OffsetStorage {

	private String m_topic;
	
	public KafkaOffsetStorage(String id) {
		m_topic = id;
	}

	@Override
	public void append(List<Offset> payloads) throws StorageException {

	}

	@Override
	public Browser<Offset> createBrowser(long offset) {
		return null;
	}

	@Override
	public List<Offset> read(Range range) throws StorageException {
		return null;
	}

	@Override
	public Offset top() throws StorageException {
		return null;
	}

	@Override
   public String getId() {
		return m_topic;
	}
}
