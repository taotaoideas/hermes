package com.ctrip.hermes.consumer;

import com.ctrip.hermes.message.PipelineContext;

public interface Consumer<T> {

	public void consume(PipelineContext<T> ctx);
	
}
