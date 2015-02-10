package com.ctrip.hermes.message.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.MessageSinkManager;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.ValveChain;
import com.ctrip.hermes.message.ValveRegistry;

public class DefaultMessagePipeline implements Pipeline<Message<Object>> {
	@Inject
	private ValveRegistry<Message<Object>> m_registry;

	@Inject
	private MessageSinkManager m_sinkManager;

	@Override
	public void put(Message<Object> message) {
		PipelineContext<Message<Object>> ctx = new PipelineContext<Message<Object>>(message);
		PipelineSink<Message<Object>> sink = m_sinkManager.getSink(message.getTopic());

		new ValveChain<Message<Object>>(m_registry.getValveList(), sink).handle(ctx);
	}

}
