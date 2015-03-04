package com.ctrip.hermes.container;

import io.netty.channel.ChannelFuture;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.MessageContext;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.message.ValveRegistry;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.ClientManager;
import com.ctrip.hermes.remoting.netty.NettyClientHandler;
import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.range.OffsetRecord;

public class BrokerConsumerBootstrap extends ContainerHolder implements LogEnabled, ConsumerBootstrap, Initializable {

	public static final String ID = "broker";

	@Inject
	private ValveRegistry m_valveRegistry;

	@Inject
	private Pipeline m_pipeline;

	@Inject
	private ClientManager m_clientManager;

	private Logger m_logger;

	private Map<Integer, SinkContext> m_consumerSinks = new ConcurrentHashMap<>();

	private Map<Integer, AckContext> m_acks = new ConcurrentHashMap<>();

	@Override
	public void startConsumer(Subscriber s) {
		NettyClientHandler netty = m_clientManager.findConsumerClient(s.getTopicPattern(), s.getGroupId());

		Command cmd = new Command(CommandType.StartConsumerRequest) //
		      .addHeader("topic", s.getTopicPattern()) //
		      .addHeader("groupId", s.getGroupId());

		LinkedBlockingQueue<OffsetRecord> ackQueue = new LinkedBlockingQueue<>();
		m_acks.put(cmd.getCorrelationId(), new AckContext(ackQueue, netty));
		m_consumerSinks.put(cmd.getCorrelationId(), new SinkContext(s, newConsumerSink(s, ackQueue)));

		ChannelFuture future = netty.writeCommand(cmd);
		
		// TODO
		future.awaitUninterruptibly();
	}

	private PipelineSink newConsumerSink(final Subscriber s, final LinkedBlockingQueue<OffsetRecord> ackQueue) {
		return new PipelineSink() {

			@SuppressWarnings({ "unchecked", "rawtypes" })
			@Override
			public void handle(PipelineContext ctx, Object payload) {
				List<StoredMessage> msgs = (List<StoredMessage>) payload;
				// TODO
				try {
					s.getConsumer().consume(msgs);
				} catch (Throwable e) {
					// TODO add more message detail
					m_logger.warn("Consumer throws exception when consuming messge", e);
				} finally {
					// TODO extract offset record from payload
					for (StoredMessage msg : msgs) {
						OffsetRecord offsetRecord = new OffsetRecord(msg.getOffset(), msg.getAckOffset());
						Ack ack = msg.isSuccess() ? Ack.SUCCESS : Ack.FAIL;
						offsetRecord.setAck(ack);
						ackQueue.offer(offsetRecord);
					}
				}
			}
		};
	}

	@Override
	public void deliverMessage(int correlationId, List<com.ctrip.hermes.storage.message.Record> msgs) {
		// TODO make it async
		SinkContext sinkCtx = m_consumerSinks.get(correlationId);
		PipelineSink sink = sinkCtx.getSink();
		Subscriber s = sinkCtx.getSubscriber();

		if (sink != null) {
			MessageContext ctx = new MessageContext(s.getTopicPattern(), msgs, s.getMessageClass());
			m_pipeline.put(new Pair<>(sink, ctx));
		} else {
			// TODO
			m_logger.error(String.format("Correlationid %s not found", correlationId));
		}
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

	@Override
	public void initialize() throws InitializationException {
		// TODO
		new Thread() {

			@Override
			public void run() {
				while (true) {
					// TODO
					for (Map.Entry<Integer, AckContext> entry : m_acks.entrySet()) {
						entry.getValue().send(entry.getKey());
					}
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
					}
				}
			}

		}.start();

	}

	private static class SinkContext {

		private Subscriber m_subscriber;

		private PipelineSink m_sink;

		public SinkContext(Subscriber subscriber, PipelineSink sink) {
			m_subscriber = subscriber;
			m_sink = sink;
		}

		public Subscriber getSubscriber() {
			return m_subscriber;
		}

		public PipelineSink getSink() {
			return m_sink;
		}

	}

	private static class AckContext {

		private LinkedBlockingQueue<OffsetRecord> m_ackQueue;

		private NettyClientHandler m_netty;

		public AckContext(LinkedBlockingQueue<OffsetRecord> ackQueue, NettyClientHandler netty) {
			m_ackQueue = ackQueue;
			m_netty = netty;
		}

		public void send(int correlationId) {
			OffsetRecord rec = null;
			while ((rec = m_ackQueue.poll()) != null) {
				// TODO
				Command cmd = new Command(CommandType.AckRequest) //
				      .setCorrelationId(correlationId) //
				      .setBody(JSON.toJSONBytes(rec));

				m_netty.writeCommand(cmd);
			}
		}

	}

}
