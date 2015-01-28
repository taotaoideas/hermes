package com.ctrip.hermes.message;


public interface MessageSinkManager {
	public MessageSink getSink(String topic);
}
