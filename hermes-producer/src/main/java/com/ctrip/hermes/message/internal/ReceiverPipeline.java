package com.ctrip.hermes.message.internal;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.ProducerMessage;
import com.ctrip.hermes.message.ValveRegistry;

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
