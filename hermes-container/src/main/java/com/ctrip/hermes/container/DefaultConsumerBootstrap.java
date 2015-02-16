package com.ctrip.hermes.container;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.BackoffException;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.MessageContext;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.ValveRegistry;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.ClientManager;
import com.ctrip.hermes.remoting.netty.NettyClientHandler;
import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.range.OffsetRecord;

public class DefaultConsumerBootstrap extends ContainerHolder implements LogEnabled, ConsumerBootstrap {

	@Inject
	private ValveRegistry m_valveRegistry;

	@Inject
	private Pipeline m_pipeline;

	@Inject
	private ClientManager m_clientManager;

	private Logger m_logger;

	private Map<Integer, PipelineSink> m_consumerSinks = new ConcurrentHashMap<>();

	private Map<Integer, BlockingQueue<AckRecord>> m_acks = new ConcurrentHashMap<>();

	@Override
	public void startConsumer(Subscriber s) {
		NettyClientHandler netty = m_clientManager.findConsumerClient(s.getTopicPattern(), s.getGroupId());

		Command cmd = new Command(CommandType.StartConsumerRequest) //
		      .addHeader("topic", s.getTopicPattern()) //
		      .addHeader("groupId", s.getGroupId());

		LinkedBlockingQueue<AckRecord> ackQueue = new LinkedBlockingQueue<>();
		m_acks.put(cmd.getCorrelationId(), ackQueue);
		m_consumerSinks.put(cmd.getCorrelationId(), newConsumerSink(s, ackQueue));

		netty.writeCommand(cmd);
	}

	private PipelineSink newConsumerSink(final Subscriber s, final LinkedBlockingQueue<AckRecord> ackQueue) {
		return new PipelineSink() {

			@SuppressWarnings({ "unchecked", "rawtypes" })
			@Override
			public void handle(PipelineContext ctx, Object payload) {
				// TODO
				Ack ack = Ack.SUCCESS;
				try {
					s.getConsumer().consume((List) payload);
				} catch (BackoffException e) {
					ack = Ack.FAIL;
				} catch (Throwable e) {
					// TODO add more message detail
					m_logger.warn("Consumer throws exception when consuming messge", e);
				} finally {
					// TODO extract offset record from payload
					OffsetRecord offsetRecord = null;
					ackQueue.offer(new AckRecord(offsetRecord, ack));
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

	private static class AckRecord {
		private OffsetRecord m_offsetRecord;

		private Ack m_ack;

		public AckRecord(OffsetRecord offsetRecord, Ack ack) {
			m_offsetRecord = offsetRecord;
			m_ack = ack;
		}

	}

}
