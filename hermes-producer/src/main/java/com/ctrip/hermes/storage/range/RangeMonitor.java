package com.ctrip.hermes.storage.range;

import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.storage.StorageException;

public interface RangeMonitor {

    void startNewRange(OffsetRecord record);

    void offsetDone(OffsetRecord record, Ack ack) throws StorageException;

    void addListener(RangeStatusListener lisener);

}
