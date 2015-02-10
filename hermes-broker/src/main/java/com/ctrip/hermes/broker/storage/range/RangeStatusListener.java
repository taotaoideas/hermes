package com.ctrip.hermes.broker.storage.range;

import com.ctrip.hermes.broker.storage.storage.StorageException;

public interface RangeStatusListener {

    public void onRangeSuccess(RangeEvent event) throws StorageException;

    public void onRangeFail(RangeEvent event) throws StorageException;

}
