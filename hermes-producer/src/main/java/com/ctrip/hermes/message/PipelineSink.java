package com.ctrip.hermes.message;

public interface PipelineSink<T> {

	public T handle(PipelineContext<T> ctx, Object payload);

}
