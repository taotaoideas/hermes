package com.ctrip.hermes.engine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.BackoffException;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.ValveRegistry;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.NettyClient;

public class DefaultConsumerBootstrap extends ContainerHolder implements LogEnabled, ConsumerBootstrap {

	@Inject
	private ValveRegistry m_valveRegistry;

	@Inject
	private Pipeline m_pipeline;

	private Logger m_logger;

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

			@SuppressWarnings({ "unchecked", "rawtypes" })
			@Override
			public void handle(PipelineContext ctx, Object payload) {
				// TODO
				try {
					s.getConsumer().consume((List) payload);
				} catch (BackoffException e) {
					// TODO send nack
				} catch (Throwable e) {
					// TODO add more message detail
					m_logger.warn("Consumer throws exception when consuming messge", e);
				}
			}
		};
	}

	@Override
	public void deliverMessage(int correlationId, MessageContext ctx) {
		// TODO make it async
		PipelineSink sink = m_consumerSinks.get(correlationId);

		if (sink != null) {
			m_pipeline.put(new Pair<>(sink, ctx));
		} else {
			// TODO
			System.out.println(String.format("Correlationid %s not found", correlationId));
		}
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

}
