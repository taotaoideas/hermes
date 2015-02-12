package com.ctrip.hermes.container;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.ValveRegistry;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.NettyClient;

public class DefaultConsumerBootstrap extends ContainerHolder implements Initializable, ConsumerBootstrap {

	@Inject
	private ValveRegistry m_valveRegistry;

	@Inject
	private Pipeline m_pipeline;

	private Map<Integer, PipelineSink> m_consumerSinks = new ConcurrentHashMap<>();

	@Override
	public void startConsumer(Subscriber s) {
		// TODO client pool
		NettyClient netty = lookup(NettyClient.class);

		Command cmd = new Command(CommandType.StartConsumerRequest) //
		      .addHeader("topic", s.getTopicPattern()) //
		      .addHeader("groupId", s.getGroupId());

		m_consumerSinks.put(cmd.getCorrelationId(), newConsumerSink(s));

		netty.start(cmd);
	}

	private PipelineSink newConsumerSink(final Subscriber s) {
		return new PipelineSink() {

			@SuppressWarnings({ "unchecked" })
			@Override
			public void handle(PipelineContext ctx, Object payload) {
				// TODO
				s.getConsumer().consume(Arrays.asList(payload));
			}
		};
	}

	@Override
	public void deliverMessage(int correlationId, byte[] body) {
		// TODO make it async
		PipelineSink sink = m_consumerSinks.get(correlationId);

		if (sink != null) {
			m_pipeline.put(new Pair<>(sink, body));
		} else {
			// TODO
			System.out.println(String.format("Correlationid %s not found", correlationId));
		}
	}

	@Override
	public void initialize() throws InitializationException {
	}

}
