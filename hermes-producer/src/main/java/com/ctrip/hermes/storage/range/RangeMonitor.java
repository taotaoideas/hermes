package com.ctrip.hermes.storage.range;

import com.ctrip.hermes.storage.storage.StorageException;

public interface RangeMonitor {

	void startNewRange(OffsetRecord record);

	void offsetDone(OffsetRecord record) throws StorageException;

	void addListener(RangeStatusListener lisener);

}
