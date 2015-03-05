package com.ctrip.hermes.message.internal;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.ValveRegistry;

public class ReceiverPipeline implements Pipeline {

	@Inject
	private ValveRegistry m_valveRegistry;

	@SuppressWarnings("unchecked")
	@Override
	public void put(Object payload) {
		Pair<Message<byte[]>, PipelineSink> pair = (Pair<Message<byte[]>, PipelineSink>) payload;

		PipelineContext ctx = new DefaultPipelineContext(m_valveRegistry.getValveList(), pair.getValue());

		ctx.next(pair.getKey());
	}

}
