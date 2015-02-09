package com.ctrip.hermes.broker.storage.storage;

import java.util.List;

public interface Browser<T> {

	List<T> read(int batchSize) throws Exception;

	void seek(long offset);
}
