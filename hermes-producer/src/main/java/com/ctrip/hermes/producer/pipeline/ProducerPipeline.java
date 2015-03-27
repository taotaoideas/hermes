package com.ctrip.hermes.producer.pipeline;

import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.pipeline.DefaultPipelineContext;
import com.ctrip.hermes.core.pipeline.Pipeline;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.pipeline.ValveRegistry;
import com.ctrip.hermes.producer.api.SendResult;

public class ProducerPipeline implements Pipeline<Future<SendResult>> {
	@Inject
	private ValveRegistry m_valveRegistry;

	@Inject
	private ProducerSinkManager m_sinkManager;

	@Override
	public Future<SendResult> put(Object payload) {
		ProducerMessage<?> msg = (ProducerMessage<?>) payload;

		String topic = msg.getTopic();
		PipelineSink<Future<SendResult>> sink = m_sinkManager.getSink(topic);
		PipelineContext<Future<SendResult>> ctx = new DefaultPipelineContext<>(m_valveRegistry.getValveList(), sink);

		ctx.next(msg);

		return ctx.getResult();
	}
}
