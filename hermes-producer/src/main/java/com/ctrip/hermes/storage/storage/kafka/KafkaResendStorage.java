package com.ctrip.hermes.storage.storage.kafka;

import java.util.List;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.spi.typed.ResendStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;
import com.google.common.collect.ImmutableMap;

public class KafkaResendStorage implements ResendStorage {

	private String m_topic;

	private KafkaProducer<String, byte[]> m_producer;

	private ConsumerConnector m_consumer;

	private int m_consumer_threads = 1;

	private ExecutorService m_executor;

	private Map<String, List<KafkaStream<byte[], byte[]>>> m_topicMessageStreams;

	public KafkaResendStorage(String id, Properties pc, Properties cc) {
		m_topic = id;
		
		if(!cc.containsKey("group.id")){
			return ;
		}
		
		m_producer = new KafkaProducer<>(pc);
		m_consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(cc));
		m_topicMessageStreams = m_consumer.createMessageStreams(ImmutableMap.of(m_topic, m_consumer_threads));
		m_executor = Executors.newFixedThreadPool(m_consumer_threads);
	}

	@Override
	public void append(List<Resend> payloads) throws StorageException {
		for (Resend payload : payloads) {
			ProducerRecord<String, byte[]> kafkaMsg = new ProducerRecord<>(m_topic, JSON.toJSONBytes(payload));
			m_producer.send(kafkaMsg);
		}
	}

	@Override
	public Browser<Resend> createBrowser(long offset) {
		long nextReadIdx = 0;
		// Ignore offset, read from beginning
		return new KafkaResendBrowser(nextReadIdx);
	}

	class KafkaResendBrowser implements Browser<Resend> {

		private long m_nextReadIdx;

		public KafkaResendBrowser(long nextReadIdx) {
			m_nextReadIdx = nextReadIdx;
		}

		@Override
		public synchronized List<Resend> read(final int batchSize) {
			final List<Resend> result = new CopyOnWriteArrayList<>();
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
								Resend resend = JSON.parseObject(msgAndMetadata.message(), Resend.class);
								m_nextReadIdx = msgAndMetadata.offset();
								result.add(resend);
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
			m_nextReadIdx = offset;
		}

		@Override
		public long currentOffset() {
			return m_nextReadIdx;
		}

	}

	@Override
	public List<Resend> read(Range range) throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Resend top() throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		return m_topic;
	}
}
