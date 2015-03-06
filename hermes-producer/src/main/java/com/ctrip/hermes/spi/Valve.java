package com.ctrip.hermes.spi;

import com.ctrip.hermes.message.PipelineContext;

public interface Valve {

	public void handle(PipelineContext<?> ctx, Object payload);

}
