package com.ctrip.hermes.broker.deliver;

public interface AckMonitor<T> {

	void delivered(EnumRange<T> range);

	void acked(Locatable<T> locatable, boolean success);

	BatchResult scan();

}
