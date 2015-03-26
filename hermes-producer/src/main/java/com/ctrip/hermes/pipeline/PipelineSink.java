package com.ctrip.hermes.pipeline;


public interface PipelineSink<T> {

	public T handle(PipelineContext<T> ctx, Object payload);

}
