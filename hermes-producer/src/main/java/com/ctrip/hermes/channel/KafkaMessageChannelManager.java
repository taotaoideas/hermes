package com.ctrip.hermes.channel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultDecoder;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.message.codec.kafka.KafkaDecoder;
import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.range.OffsetRecord;

public class KafkaMessageChannelManager implements MessageChannelManager, LogEnabled {

	public static final String ID = "kafka";

	@Inject
	private MetaService m_meta;

	private Map<Pair<String, String>, List<ConsumerChannelHandler>> m_handlers = new HashMap<>();

	private Logger m_logger;

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

	@Override
	public synchronized ConsumerChannel newConsumerChannel(final String topic, final String groupId) {
		Properties props = new Properties();
		// TODO what parameter?
		int numStreams = 1;
		Storage storage = m_meta.getStorage(topic);
		for (Property prop : storage.getProperties()) {
			props.put(prop.getName(), prop.getValue());
		}
		props.put("group.id", groupId);

		// Create the connection to the cluster
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		final ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topic, numStreams);
		Map<String, List<KafkaStream<byte[], Message>>> topicMessageStreams = consumerConnector.createMessageStreams(
		      topicCountMap, new DefaultDecoder(null), new KafkaDecoder(null));
		final List<KafkaStream<byte[], Message>> streams = topicMessageStreams.get(topic);

		return new ConsumerChannel() {
			@Override
			public void close() {
				consumerConnector.shutdown();
			}

			@Override
			public void start(ConsumerChannelHandler handler) {
				synchronized (KafkaMessageChannelManager.this) {
					Pair<String, String> pair = new Pair<>(topic, groupId);
					List<ConsumerChannelHandler> curHandlers = m_handlers.get(pair);

					if (curHandlers == null) {
						curHandlers = startQueuePuller(streams);
						m_handlers.put(pair, curHandlers);
					}
					curHandlers.add(handler);
				}
			}

			@Override
			public void ack(List<OffsetRecord> recs) {
				m_logger.info("ACK..." + recs);
				// consumerConnector.commitOffsets();
			}
		};

	}

	private List<ConsumerChannelHandler> startQueuePuller(final List<KafkaStream<byte[], Message>> streams) {
		final List<ConsumerChannelHandler> handlers = Collections
		      .synchronizedList(new ArrayList<ConsumerChannelHandler>());
		final ExecutorService executor = Executors.newFixedThreadPool(streams.size());
		for (final KafkaStream<byte[], Message> stream : streams) {
			executor.submit(new Runnable() {
				public void run() {
					int m_idx = 0;
					for (MessageAndMetadata<byte[], Message> msgAndMetadata : stream) {
						while (true) {
							if (handlers.isEmpty()) {
								try {
									TimeUnit.SECONDS.sleep(1);
								} catch (InterruptedException e) {
								}
							} else {
								int curIdx = m_idx++;
								ConsumerChannelHandler handler = handlers.get(curIdx % handlers.size());
								if (handler.isOpen()) {
									// TODO handle exception
									handler.handle(Arrays.asList(msgAndMetadata.message()));
									break;
								} else {
									m_logger.info(String.format("Remove closed consumer handler %s", handler));
									handlers.remove(handler);
									continue;
								}
							}
						}
					}
				}
			});
		}

		return handlers;
	}

	@Override
	public ProducerChannel newProducerChannel(final String topic) {
		Properties props = new Properties();
		Storage storage = m_meta.getStorage(topic);
		for (Property prop : storage.getProperties()) {
			props.put(prop.getName(), prop.getValue());
		}
		props.put("serializer.class", "com.ctrip.hermes.message.codec.kafka.KafkaEncoder");
		props.put("key.serializer.class", "kafka.serializer.DefaultEncoder");
		ProducerConfig config = new ProducerConfig(props);

		final Producer<String, Message> producer = new Producer<>(config);

		return new ProducerChannel() {

			@Override
			public void send(List<Message> msgs) {
				for (Message msg : msgs) {
					KeyedMessage<String, Message> kafkaMsg = new KeyedMessage<>(topic, msg);
					producer.send(kafkaMsg);
				}
			}

			@Override
			public void close() {
				producer.close();
			}

		};
	}

}
