package com.ctrip.hermes.message;

public interface MessagePipeline {

	public void put(Message<Object> message);
}
