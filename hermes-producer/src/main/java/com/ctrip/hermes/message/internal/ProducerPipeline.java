package com.ctrip.hermes.message.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.ProducerSinkManager;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.ValveRegistry;

public class ProducerPipeline implements Pipeline {
	@Inject
	private ValveRegistry m_registry;

	@Inject
	private ProducerSinkManager m_sinkManager;

	@SuppressWarnings("unchecked")
	@Override
	public void put(Object payload) {
		Message<Object> msg = (Message<Object>) payload;

		PipelineSink sink = m_sinkManager.getSink(msg.getTopic());
		PipelineContext ctx = new DefaultPipelineContext(m_registry.getValveList(), sink);

		ctx.next(msg);
	}
}
