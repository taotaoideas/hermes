package com.ctrip.hermes.pipeline.spi;

import com.ctrip.hermes.pipeline.PipelineContext;

public interface Valve {

	public void handle(PipelineContext<?> ctx, Object payload);

}
