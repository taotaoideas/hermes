package com.ctrip.hermes.message;

public interface PipelineSink {

	public void handle(PipelineContext ctx, Object payload);

}
