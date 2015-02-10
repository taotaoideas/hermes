package com.ctrip.hermes.message;

public interface PipelineSink<T> {

	public void handle(PipelineContext<T> ctx);

}
