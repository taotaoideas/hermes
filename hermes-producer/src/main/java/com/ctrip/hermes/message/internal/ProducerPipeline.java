package com.ctrip.hermes.message.internal;

import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.ProducerSinkManager;
import com.ctrip.hermes.message.ValveRegistry;

public class ProducerPipeline implements Pipeline<Future<SendResult>> {
	@Inject
	private ValveRegistry m_registry;

	@Inject
	private ProducerSinkManager m_sinkManager;

	@SuppressWarnings("unchecked")
	@Override
	public Future<SendResult> put(Object payload) {
		Message<Object> msg = (Message<Object>) payload;

		String topic = msg.getTopic();
		PipelineSink<Future<SendResult>> sink = m_sinkManager.getSink(topic);
		PipelineContext<Future<SendResult>> ctx = new DefaultPipelineContext<>(m_registry.getValveList(), sink);
		ctx.put("topic", topic);

		ctx.next(msg);

		return ctx.getResult();
	}
}
