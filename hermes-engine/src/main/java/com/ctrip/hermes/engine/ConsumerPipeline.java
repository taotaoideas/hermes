package com.ctrip.hermes.engine;

import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.ValveRegistry;
import com.ctrip.hermes.message.internal.DefaultPipelineContext;
import com.ctrip.hermes.spi.Valve;

public class ConsumerPipeline implements Pipeline<Void> {

	@Inject
	private ValveRegistry m_registry;

	@SuppressWarnings("unchecked")
	@Override
	public Void put(Object payload) {
		Pair<PipelineSink<Void>, Object> pair = (Pair<PipelineSink<Void>, Object>) payload;

		List<Valve> valves = m_registry.getValveList();
		PipelineContext<Void> ctx = new DefaultPipelineContext<>(valves, pair.getKey());

		ctx.next(pair.getValue());

		return null;
	}

}
