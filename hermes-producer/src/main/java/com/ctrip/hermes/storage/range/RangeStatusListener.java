package com.ctrip.hermes.storage.range;

import com.ctrip.hermes.storage.storage.StorageException;

public interface RangeStatusListener {

    public void onRangeSuccess(RangeEvent event) throws StorageException;

    public void onRangeFail(RangeEvent event) throws StorageException;

}
