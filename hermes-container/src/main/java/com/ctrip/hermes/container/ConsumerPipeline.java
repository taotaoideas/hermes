package com.ctrip.hermes.container;

import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.ValveRegistry;
import com.ctrip.hermes.message.internal.DefaultPipelineContext;
import com.ctrip.hermes.spi.Valve;

public class ConsumerPipeline implements Pipeline {

	@Inject
	private ValveRegistry m_registry;

	@SuppressWarnings("unchecked")
	@Override
	public void put(Object payload) {
		Pair<PipelineSink, Object> pair = (Pair<PipelineSink, Object>) payload;

		List<Valve> valves = m_registry.getValveList();
		PipelineContext ctx = new DefaultPipelineContext(valves, pair.getKey());

		ctx.next(pair.getValue());
	}

}
