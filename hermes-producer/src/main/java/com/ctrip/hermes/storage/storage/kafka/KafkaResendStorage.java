package com.ctrip.hermes.storage.storage.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.spi.typed.ResendStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;

public class KafkaResendStorage implements ResendStorage {

	private static final Logger m_logger = Logger.getLogger(KafkaResendStorage.class);

	private String m_topic;

	private KafkaProducer<String, byte[]> m_producer;

	private Resend m_last_resend;

	private ConsumerConnector m_consumer;

	private static final int CONSUMER_THREADS = 1;

	private KafkaStream<byte[], byte[]> m_topicMessageStream;

	public KafkaResendStorage(String id, Properties pc, Properties cc) {
		m_topic = id;

		if (!cc.containsKey("group.id")) {
			return;
		}

		m_producer = new KafkaProducer<>(pc);
		m_consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(cc));
		Map<String, Integer> topicCount = new HashMap<>();
		topicCount.put(m_topic, CONSUMER_THREADS);
		Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = m_consumer.createMessageStreams(topicCount);
		m_topicMessageStream = topicMessageStreams.get(m_topic).get(0);
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
		return new KafkaResendBrowser(m_topicMessageStream);
	}

	class KafkaResendBrowser implements Browser<Resend> {

		private long m_nextReadIdx;

		private ConsumerIterator<byte[], byte[]> topicStreamIterator;

		public KafkaResendBrowser(KafkaStream<byte[], byte[]> topicMessageStream) {
			if (topicMessageStream != null)
				topicStreamIterator = topicMessageStream.iterator();
		}

		@Override
		public synchronized List<Resend> read(final int batchSize) {
			List<Resend> result = new ArrayList<>();
			try {
				if (topicStreamIterator == null || topicStreamIterator.isEmpty())
					return result;
				while (topicStreamIterator.hasNext()) {
					MessageAndMetadata<byte[], byte[]> msgAndMetadata = topicStreamIterator.next();
					Resend resend = JSON.parseObject(msgAndMetadata.message(), Resend.class);
					m_nextReadIdx = msgAndMetadata.offset();
					m_last_resend = resend;
					result.add(resend);
					if (result.size() == batchSize) {
						break;
					}
				}
			} catch (ConsumerTimeoutException e) {
				return result;
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
		return m_last_resend;
	}

	@Override
	public String getId() {
		return m_topic;
	}

}
