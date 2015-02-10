package com.ctrip.hermes.broker.storage.range;

import com.ctrip.hermes.broker.storage.message.Ack;
import com.ctrip.hermes.broker.storage.storage.StorageException;

public interface RangeMonitor {

    void startNewRange(OffsetRecord record);

    void offsetDone(OffsetRecord record, Ack ack) throws StorageException;

    void addListener(RangeStatusListener lisener);

}
