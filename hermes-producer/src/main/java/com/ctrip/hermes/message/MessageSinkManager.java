package com.ctrip.hermes.message;


public interface MessageSinkManager {
	public PipelineSink<Message<Object>> getSink(String topic);
}
