package com.ctrip.hermes.message.internal;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.pipeline.DefaultPipelineContext;
import com.ctrip.hermes.core.pipeline.Pipeline;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.pipeline.ValveRegistry;
import com.ctrip.hermes.producer.ProducerMessage;

public class ReceiverPipeline implements Pipeline<Void> {

	@Inject
	private ValveRegistry m_valveRegistry;

	@SuppressWarnings("unchecked")
	@Override
	public Void put(Object payload) {
		Pair<ProducerMessage<byte[]>, PipelineSink<Void>> pair = (Pair<ProducerMessage<byte[]>, PipelineSink<Void>>) payload;

		PipelineContext<Void> ctx = new DefaultPipelineContext<>(m_valveRegistry.getValveList(), pair.getValue());

		ctx.next(pair.getKey());

		return null;
	}

}
