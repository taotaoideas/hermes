package com.ctrip.hermes.message.internal;

import com.ctrip.hermes.pipeline.PipelineContext;
import com.ctrip.hermes.pipeline.spi.Valve;

public class DeliverTracingValve implements Valve {

	public static final String ID = "deliver-tracing";

	@Override
	public void handle(PipelineContext<?> ctx, Object payload) {

	}

}
