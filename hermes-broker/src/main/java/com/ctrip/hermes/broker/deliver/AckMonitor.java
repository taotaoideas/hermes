package com.ctrip.hermes.broker.deliver;

import java.util.List;

import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.storage.Locatable;

public interface AckMonitor<T> {

	void delivered(List<Locatable> locatables, T ctx);

	void acked(Locatable locatable, Ack ack);

	void addListener(AckStatusListener<T> listener);

}
