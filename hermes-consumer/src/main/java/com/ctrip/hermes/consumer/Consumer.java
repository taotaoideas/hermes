package com.ctrip.hermes.consumer;

import java.util.List;

public interface Consumer<T> {

	public void consume(List<Message<T>> msgs) throws BackoffException;
	
}
