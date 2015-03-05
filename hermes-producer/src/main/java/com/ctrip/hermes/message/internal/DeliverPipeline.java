package com.ctrip.hermes.message.internal;

import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.message.ValveRegistry;

public class DeliverPipeline implements Pipeline {

	@Inject
	private ValveRegistry m_valveRegistry;

	@SuppressWarnings("unchecked")
	@Override
	public void put(Object payload) {
		Pair<List<StoredMessage<byte[]>>, PipelineSink> pair = (Pair<List<StoredMessage<byte[]>>, PipelineSink>) payload;

		PipelineContext ctx = new DefaultPipelineContext(m_valveRegistry.getValveList(), pair.getValue());

		ctx.next(pair.getKey());
	}

}
