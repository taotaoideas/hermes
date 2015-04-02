package com.ctrip.hermes.engine;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.KafkaConsumerMessage;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.message.codec.MessageCodecFactory;
import com.ctrip.hermes.engine.bootstrap.BaseConsumerBootstrap;
import com.ctrip.hermes.engine.bootstrap.ConsumerBootstrap;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

@Named(type = ConsumerBootstrap.class, value = Endpoint.KAFKA)
public class KafkaConsumerBootstrap extends BaseConsumerBootstrap implements LogEnabled {

	private Logger m_logger;

	private ExecutorService m_executor = Executors.newCachedThreadPool();

	@Override
	protected void doStart(ConsumerContext consumerContext) {
		Topic topic = consumerContext.getTopic();

		Properties prop = getConsumerProperties(topic.getName(), consumerContext.getGroupId());
		ConsumerConnector consumerConnector = kafka.consumer.Consumer
		      .createJavaConsumerConnector(new ConsumerConfig(prop));
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topic.getName(), 1);
		List<KafkaStream<byte[], byte[]>> streams = consumerConnector.createMessageStreams(topicCountMap).get(topic.getName());
		KafkaStream<byte[], byte[]> stream = streams.get(0);

		m_consumerNotifier.register(stream.hashCode(), consumerContext);
		m_executor.submit(new KafkaConsumerThread(stream, consumerContext));
	}

	class KafkaConsumerThread implements Runnable {

		private KafkaStream<byte[], byte[]> stream;

		private ConsumerContext consumerContext;

		private MessageCodec codec;

		private long correlationId;

		public KafkaConsumerThread(KafkaStream<byte[], byte[]> stream, ConsumerContext consumerContext) {
			this.stream = stream;
			this.consumerContext = consumerContext;
			this.codec = MessageCodecFactory.getCodec(consumerContext.getTopic().getName());
			correlationId = stream.hashCode();
		}

		@Override
		public void run() {
			for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
				try {
					ByteBuf byteBuf = Unpooled.wrappedBuffer(msgAndMetadata.message());

					BaseConsumerMessage<?> baseMsg = codec.decode(byteBuf, consumerContext.getMessageClazz());
					ConsumerMessage kafkaMsg = new KafkaConsumerMessage(baseMsg);
					List<ConsumerMessage<?>> msgs = new ArrayList<>();
					msgs.add(kafkaMsg);
					m_consumerNotifier.messageReceived(correlationId, msgs);
				} catch (Exception e) {
					m_logger.warn("", e);
				}
			}
		}
	}

	private Properties getConsumerProperties(String topic, String group) {
		Properties configs = new Properties();
		List<Partition> partitions = m_metaService.getPartitions(topic);
		if (partitions == null || partitions.size() < 1) {
			return configs;
		}

		String consumerDatasource = partitions.get(0).getReadDatasource();
		Storage targetStorage = m_metaService.findStorage(topic);
		if (targetStorage == null) {
			return configs;
		}

		for (Datasource datasource : targetStorage.getDatasources()) {
			if (consumerDatasource.equals(datasource.getId())) {
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

}
