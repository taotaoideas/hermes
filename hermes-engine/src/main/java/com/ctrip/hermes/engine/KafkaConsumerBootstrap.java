package com.ctrip.hermes.engine;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.message.DecodedProducerMessage;
import com.ctrip.hermes.core.message.codec.ProducerMessageCodec;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.pipeline.Pipeline;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.pipeline.ValveRegistry;
import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;

public class KafkaConsumerBootstrap extends ContainerHolder implements ConsumerBootstrap, LogEnabled {

	public static final String ID = "kafka";

	@Inject
	private ValveRegistry m_valveRegistry;

	@Inject
	private Pipeline<Void> m_pipeline;

	@Inject
	private ProducerMessageCodec m_codec;

	@Inject
	private MetaService m_metaService;

	private Logger m_logger;

	private ExecutorService m_executor = Executors.newCachedThreadPool();

	@Override
	public void startConsumer(final Subscriber s) {
		String topicPattern = s.getTopicPattern();

		Properties prop = getConsumerProperties(s.getTopicPattern(), s.getGroupId());
		ConsumerConnector consumerConnector = kafka.consumer.Consumer
		      .createJavaConsumerConnector(new ConsumerConfig(prop));
		List<KafkaStream<byte[], byte[]>> streams = consumerConnector.createMessageStreamsByFilter(new Whitelist(
		      topicPattern), 1);
		KafkaStream<byte[], byte[]> stream = streams.get(0);

		m_executor.submit(new KafkaConsumerThread(stream, new SinkContext(s, newConsumerSink(s))));
	}

	class KafkaConsumerThread implements Runnable {

		private KafkaStream<byte[], byte[]> stream;

		private SinkContext sinkCtx;

		public KafkaConsumerThread(KafkaStream<byte[], byte[]> stream, SinkContext sinkCtx) {
			this.stream = stream;
			this.sinkCtx = sinkCtx;
		}

		@Override
		public void run() {
			for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
				try {
					// store msg list
					byte[] bytes = msgAndMetadata.message();
					ByteBuf byteBuf = Unpooled.copiedBuffer(bytes);
					DecodedProducerMessage msg = m_codec.decode(byteBuf);

//					for (StoredMessage storedMsg : storedMsgs) {
//						storedMsg.setPartition(String.valueOf(msgAndMetadata.partition()));
//					}
//					PipelineSink<Void> sink = sinkCtx.getSink();
//					Subscriber s = sinkCtx.getSubscriber();
//
//					MessageContext ctx = new MessageContext(s.getTopicPattern(), storedMsgs, s.getMessageClass());
//					m_pipeline.put(new Pair<>(sink, ctx));
				} catch (Exception e) {
					m_logger.warn("", e);
				}
			}
		}
	}

	private PipelineSink<Void> newConsumerSink(final Subscriber s) {

		return new PipelineSink<Void>() {

			@SuppressWarnings({ "unchecked", "rawtypes" })
			@Override
			public Void handle(PipelineContext ctx, Object payload) {
				List<StoredMessage> msgs = (List<StoredMessage>) payload;
				try {
					s.getConsumer().consume(msgs);
				} catch (Throwable e) {
					m_logger.warn("Consumer throws exception when consuming messge", e);
				}

				return null;
			}
		};
	}

	private Properties getConsumerProperties(String topic, String group) {
		Properties configs = new Properties();
		List<Partition> partitions = m_metaService.getPartitions(topic);
		String ds = partitions.get(0).getReadDatasource();
		Storage targetStorage = m_metaService.findStorage(topic);
		for (Datasource datasource : targetStorage.getDatasources()) {
			if (ds.equals(datasource.getId())) {
				for (Property prop : datasource.getProperties()) {
					configs.put(prop.getName(), prop.getValue());
				}
				break;
			}
		}
		configs.put("group.id", group);
		return configs;
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

	@Override
	public void deliverMessage(long correlationId, List<StoredMessage<byte[]>> msgs) {
		// TODO Auto-generated method stub

	}

	private static class SinkContext {

		private Subscriber m_subscriber;

		private PipelineSink<Void> m_sink;

		public SinkContext(Subscriber subscriber, PipelineSink<Void> sink) {
			m_subscriber = subscriber;
			m_sink = sink;
		}

		public Subscriber getSubscriber() {
			return m_subscriber;
		}

		public PipelineSink<Void> getSink() {
			return m_sink;
		}

	}
}
