package com.ctrip.hermes.storage.storage.kafka;

import java.util.List;

import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.spi.typed.ResendStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;

public class KafkaResendStorage implements ResendStorage {

	@Override
	public void append(List<Resend> payloads) throws StorageException {
		// TODO Auto-generated method stub

	}

	@Override
	public Browser<Resend> createBrowser(long offset) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Resend> read(Range range) throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Resend top() throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		// TODO Auto-generated method stub
		return null;
	}

}
