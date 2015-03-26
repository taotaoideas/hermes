package com.ctrip.hermes.broker.deliver;

import com.ctrip.hermes.storage.storage.Range;

public interface AckStatusListener<T> {

	public void onSuccess(Range range, T ctx);

	public void onFail(Range range, T ctx);

}
