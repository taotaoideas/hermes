package com.ctrip.hermes.engine;

import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.spi.Valve;

public class ConsumerTracingValve implements Valve {

	public static final String ID = "consumer-tracing";

	@Override
	public void handle(PipelineContext ctx, Object payload) {
		ctx.next(payload);
	}

}
