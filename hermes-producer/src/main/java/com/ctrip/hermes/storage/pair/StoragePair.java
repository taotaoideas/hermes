package com.ctrip.hermes.storage.pair;

import java.util.List;

import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.range.OffsetRecord;
import com.ctrip.hermes.storage.range.RangeStatusListener;
import com.ctrip.hermes.storage.storage.Locatable;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;

public interface StoragePair<T extends Locatable> {
	public List<T> readMain(int batchSize) throws StorageException;

	public List<T> readMain(Range r) throws StorageException;

	public void appendMain(List<T> payloads) throws StorageException;

	public void appendMain(T payload) throws StorageException;

	public void ack(OffsetRecord record) throws StorageException;

	public List<String> getStorageIds();

	public void addRangeStatusListener(RangeStatusListener listener);

	public void waitForAck(List<Message> msgs);

	public void waitForAck(List<Message> msgs, Offset offset);
}
