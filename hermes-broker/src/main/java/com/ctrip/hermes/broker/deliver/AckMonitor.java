package com.ctrip.hermes.broker.deliver;


public interface AckMonitor<T> {

	void delivered(EnumRange<?> range, T ctx);

	void acked(Locatable locatable, boolean success);

	void addListener(AckStatusListener<T> listener);

}
