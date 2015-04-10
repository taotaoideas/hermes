package com.ctrip.hermes.broker.deliver;

public interface AckStatusListener<T> {

	public void onSuccess(ContinuousRange<?> range, T ctx);

	public void onFail(EnumRange<?> range, T ctx);

}
