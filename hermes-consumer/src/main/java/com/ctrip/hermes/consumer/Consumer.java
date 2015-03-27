package com.ctrip.hermes.consumer;

import java.util.List;

import com.ctrip.hermes.core.message.ConsumerMessage;

public interface Consumer<T> {

	public void consume(List<ConsumerMessage<T>> msgs);

}
