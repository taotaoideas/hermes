package com.ctrip.hermes.consumer;

import java.util.List;

import com.ctrip.hermes.message.Message;

public interface Consumer<T> {

	public void consume(List<Message<T>> msgs);

}
