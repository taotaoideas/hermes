package com.ctrip.hermes.engine.pipeline.internal;

import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.spi.Valve;

public class ConsumerTracingValve implements Valve {

	public static final String ID = "consumer-tracing";

	@Override
	public void handle(PipelineContext<?> ctx, Object payload) {
		ctx.next(payload);
	}

}
