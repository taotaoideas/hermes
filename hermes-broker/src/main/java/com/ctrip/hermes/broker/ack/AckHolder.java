package com.ctrip.hermes.broker.ack;

public interface AckHolder {

	void delivered(EnumRange range);

	void acked(long offset, boolean success);

	BatchResult scan();

}
