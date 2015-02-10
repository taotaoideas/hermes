package com.ctrip.hermes.spi;

import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.ValveChain;

public interface Valve<T> {

	public void handle(ValveChain<T> chain, PipelineContext<T> ctx);

}
