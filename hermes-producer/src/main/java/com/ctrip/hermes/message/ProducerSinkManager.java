package com.ctrip.hermes.message;

public interface ProducerSinkManager {
	public PipelineSink getSink(String topic);
}
