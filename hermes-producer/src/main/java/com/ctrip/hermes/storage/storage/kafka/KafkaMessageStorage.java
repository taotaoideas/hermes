package com.ctrip.hermes.storage.storage.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.spi.typed.MessageStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;
import com.google.common.collect.ImmutableMap;

public class KafkaMessageStorage implements MessageStorage {

	private String m_topic;

	private Producer<String, byte[]> m_producer;

	private ConsumerConnector m_consumer;

	private ExecutorService m_executor;

	private int m_consumer_threads = 1;

	private Record m_last_msg;

	private Map<String, List<KafkaStream<byte[], byte[]>>> m_topicMessageStreams;

	public KafkaMessageStorage(String topic, ProducerConfig pc, ConsumerConfig cc) {
		m_topic = topic;
		m_producer = new Producer<>(pc);
		m_consumer = Consumer.createJavaConsumerConnector(cc);
		m_topicMessageStreams = m_consumer.createMessageStreams(ImmutableMap.of(m_topic, m_consumer_threads));
		m_executor = Executors.newFixedThreadPool(m_consumer_threads);
	}

	@Override
	public void append(List<Record> payloads) throws StorageException {
		for (Record payload : payloads) {
			KeyedMessage<String, byte[]> kafkaMsg = new KeyedMessage<>(m_topic, payload.getPartition(),
			      payload.getContent());
			m_producer.send(kafkaMsg);
		}
	}

	@Override
	public Browser<Record> createBrowser(long offset) {
		// FIXME unsupport offset still
		return new KafkaMessageBrowser();
	}

	class KafkaMessageBrowser implements Browser<Record> {

		private long m_nextReadIdx;

		public KafkaMessageBrowser() {
		}

		@Override
		public synchronized List<Record> read(final int batchSize) {
			final List<Record> result = new CopyOnWriteArrayList<>();
			List<KafkaStream<byte[], byte[]>> streams = m_topicMessageStreams.get(m_topic);
			final CountDownLatch latch = new CountDownLatch(m_consumer_threads);
			for (final KafkaStream<byte[], byte[]> stream : streams) {
				m_executor.submit(new Runnable() {
					public void run() {
						try {
							for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
								if (result.size() == batchSize) {
									break;
								}
								Record msg = new Record();
								msg.setContent(msgAndMetadata.message());
								msg.setOffset(new Offset(msgAndMetadata.topic(), msgAndMetadata.offset()));
								msg.setPartition(String.valueOf(msgAndMetadata.partition()));
								m_nextReadIdx = msgAndMetadata.offset();
								result.add(msg);
								m_last_msg = msg;
								if (result.size() == batchSize) {
									break;
								}
							}
						} finally {
							latch.countDown();
						}
					}
				});
			}
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return result;
		}

		@Override
		public synchronized void seek(long offset) {
		}

		@Override
		public long currentOffset() {
			return m_nextReadIdx;
		}

	}

	@Override
	public List<Record> read(Range range) throws StorageException {
		// FIXME Unsupported
		List<Record> result = new ArrayList<>();
		return result;
	}

	@Override
	public Record top() throws StorageException {
		return m_last_msg;
	}

	@Override
	public String getId() {
		return m_topic;
	}
}