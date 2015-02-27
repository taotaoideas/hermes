package com.ctrip.hermes.storage.storage.kafka;

import java.util.List;

import com.ctrip.hermes.storage.spi.typed.OffsetStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;

public class KafkaOffsetStorage implements OffsetStorage {

	@Override
	public void append(List<Offset> payloads) throws StorageException {
		// TODO Auto-generated method stub

	}

	@Override
	public Browser<Offset> createBrowser(long offset) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Offset> read(Range range) throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Offset top() throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		// TODO Auto-generated method stub
		return null;
	}

}
