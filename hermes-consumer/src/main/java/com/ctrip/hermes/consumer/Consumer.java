package com.ctrip.hermes.consumer;

import java.util.List;

import com.ctrip.hermes.message.StoredMessage;

public interface Consumer<T> {

	public void consume(List<StoredMessage<T>> msgs);

}
