package com.ctrip.hermes.message;

public interface MessagePipeline {

	public void put(MessageContext<Object> ctx);
}
