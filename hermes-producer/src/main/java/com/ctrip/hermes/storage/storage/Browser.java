package com.ctrip.hermes.storage.storage;

import java.util.List;

public interface Browser<T> {

	List<T> read(int batchSize) throws Exception;

	void seek(long offset);

	long currentOffset();
}
