package com.ctrip.hermes.consumer.engine.pipeline;

import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.pipeline.DefaultPipelineContext;
import com.ctrip.hermes.core.pipeline.Pipeline;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.pipeline.ValveRegistry;
import com.ctrip.hermes.core.pipeline.spi.Valve;

@Named(type = Pipeline.class, value = ConsumerPipeline.CONSUMER)
public class ConsumerPipeline implements Pipeline<Void> {

	public static final String CONSUMER = "consumer";

	@Inject(CONSUMER)
	private ValveRegistry m_registry;

	@SuppressWarnings("unchecked")
	@Override
	public Void put(Object payload) {

		List<Valve> valves = m_registry.getValveList();
		PipelineContext<Void> ctx = new DefaultPipelineContext<>(valves, new PipelineSink<Void>() {

			@Override
			public Void handle(PipelineContext<Void> ctx, Object payload) {
				Pair<ConsumerContext, List<ConsumerMessage<?>>> pair = (Pair<ConsumerContext, List<ConsumerMessage<?>>>) payload;
				pair.getKey().getConsumer().consume(pair.getValue());
				return null;
			}
		});

		ctx.next(payload);

		return null;
	}

}
