package com.ctrip.hermes.storage.storage.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.spi.typed.MessageStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;

public class KafkaMessageStorage implements MessageStorage {

	private static final Logger m_logger = Logger.getLogger(KafkaMessageStorage.class);

	private String m_topic;

	private KafkaProducer<String, byte[]> m_producer;

	private KafkaSendMessageCallback m_send_callback;

	private ConsumerConnector m_consumer;

	private static final int CONSUMER_THREADS = 1;

	private Record m_last_msg;

	private KafkaStream<byte[], byte[]> m_topicMessageStream;

	private List<Pair<String, Integer>> brokers = new ArrayList<>();

	private Map<Integer, SimpleConsumer> m_consumers;

	private int bufferSize = 10000;

	public KafkaMessageStorage(String topic, Properties pc, Properties cc) {
		m_topic = topic;

		if (!cc.containsKey("group.id")) {
			// Producer only
			m_producer = new KafkaProducer<>(pc);
			m_send_callback = new KafkaSendMessageCallback();
			return;
		}

		m_consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(cc));
		Map<String, Integer> topicCount = new HashMap<>();
		topicCount.put(m_topic, CONSUMER_THREADS);
		Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = m_consumer.createMessageStreams(topicCount);
		m_topicMessageStream = topicMessageStreams.get(m_topic).get(0);
		m_consumers = new HashMap<>();

		String[] brokerList = null;
		if (cc.containsKey("metadata.broker.list"))
			brokerList = cc.getProperty("metadata.broker.list").split(",");
		else if (cc.containsKey("bootstrap.servers"))
			brokerList = cc.getProperty("bootstrap.servers").split(",");
		for (String broker : brokerList) {
			brokers.add(new Pair<>(broker.split(":")[0], Integer.parseInt(broker.split(":")[1])));
		}
		TopicMetadata topicMetadata = SimpleConsumerUtil
		      .getTopicMetadata(brokers.get(0).getKey(), brokers.get(0).getValue(), m_topic).topicsMetadata().get(0);
		if (topicMetadata.topic() == null) {
			m_logger.warn(String.format("Can't find topic %s. Exiting", m_topic));
			return;
		}
		if (topicMetadata.partitionsMetadata().size() == 0) {
			m_logger.warn(String.format("Can't find partition for topic %s. Exiting", m_topic));
			return;
		}

		for (PartitionMetadata partitionData : topicMetadata.partitionsMetadata()) {
			PartitionMetadata metadata = SimpleConsumerUtil.findLeader(brokers, m_topic, partitionData.partitionId());
			if (metadata == null) {
				m_logger.warn(String.format("Can't find metadata for topic %s and partition %s. Exiting", m_topic,
				      partitionData.partitionId()));
				return;
			}
			if (metadata.leader() == null) {
				m_logger.warn(String.format("Can't find leader for topic %s and partition %s. Exiting", m_topic,
				      partitionData.partitionId()));
				return;
			}
			String leadBroker = metadata.leader().host();
			String clientName = "Client_" + m_topic + "_" + partitionData.partitionId();

			SimpleConsumer consumer = new SimpleConsumer(leadBroker, metadata.leader().port(), bufferSize, 64 * 1024,
			      clientName);
			m_consumers.put(partitionData.partitionId(), consumer);
		}
	}

	@Override
	public void append(List<Record> payloads) throws StorageException {
		for (Record payload : payloads) {
			ProducerRecord<String, byte[]> kafkaMsg = new ProducerRecord<>(m_topic, payload.getPartition(),
			      payload.getContent());
			m_producer.send(kafkaMsg, m_send_callback);
		}
	}

	class KafkaSendMessageCallback implements Callback {

		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			// TODO Auto-generated method stub

		}

	}

	@Override
	public Browser<Record> createBrowser(long offset) {
		// FIXME unsupport offset still
		return new KafkaMessageBrowser(m_topicMessageStream);
	}

	class KafkaMessageBrowser implements Browser<Record> {

		private long m_nextReadIdx;

		private ConsumerIterator<byte[], byte[]> topicStreamIterator;

		public KafkaMessageBrowser(KafkaStream<byte[], byte[]> topicMessageStream) {
			if (topicMessageStream != null)
				topicStreamIterator = topicMessageStream.iterator();
		}

		@Override
		public synchronized List<Record> read(final int batchSize) {
			List<Record> result = new ArrayList<>();
			try {
				if (topicStreamIterator == null || topicStreamIterator.isEmpty())
					return result;

				while (topicStreamIterator.hasNext()) {
					MessageAndMetadata<byte[], byte[]> msgAndMetadata = topicStreamIterator.next();
					Record msg = new Record();
					msg.setContent(msgAndMetadata.message());
					msg.setOffset(new Offset(String.valueOf(msgAndMetadata.partition()), msgAndMetadata.offset()));
					msg.setPartition(String.valueOf(msgAndMetadata.partition()));
					m_nextReadIdx = msgAndMetadata.offset();
					result.add(msg);
					m_last_msg = msg;
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
		SimpleConsumer consumer = m_consumers.get(Integer.parseInt(range.getId()));
		if (consumer == null) {
			m_logger.warn(String.format("Wrong range Id: %s", range.getId()));
			return result;
		}

		long nextReadIdx = range.getStartOffset().getOffset();
		int numErrors = 0;
		while (true) {
			FetchRequest req = new FetchRequestBuilder().clientId(consumer.clientId())
			      .addFetch(m_topic, Integer.parseInt(range.getId()), nextReadIdx, bufferSize).build();
			FetchResponse fetchResponse = consumer.fetch(req);
			if (fetchResponse.hasError()) {
				numErrors++;
				short code = fetchResponse.errorCode(m_topic, Integer.parseInt(range.getId()));
				m_logger.warn("Error fetching data Reason: " + code);
				if (numErrors > 5)
					break;
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					nextReadIdx = SimpleConsumerUtil.getLastOffset(consumer, m_topic, Integer.parseInt(range.getId()),
					      kafka.api.OffsetRequest.LatestTime(), consumer.clientId());
					continue;
				}
				consumer.close();
				PartitionMetadata metadata = SimpleConsumerUtil.findLeader(brokers, m_topic,
				      Integer.parseInt(range.getId()));
				String leadBroker = metadata.leader().host();
				String clientName = "Client_" + m_topic + "_" + range.getId();

				consumer = new SimpleConsumer(leadBroker, metadata.leader().port(), bufferSize, 64 * 1024, clientName);
				continue;
			}
			numErrors = 0;

			ByteBufferMessageSet messageSet = fetchResponse.messageSet(m_topic, Integer.parseInt(range.getId()));
			if (messageSet.sizeInBytes() == 0) {
				break;
			}
			for (MessageAndOffset messageAndOffset : messageSet) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < nextReadIdx) {
					m_logger.debug(String.format("Found an old offset: %s Expecting: %s", currentOffset, nextReadIdx));
					continue;
				}
				nextReadIdx = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				Record record = new Record();
				record.setContent(bytes);
				record.setPartition(range.getId());
				record.setOffset(new Offset(range.getId(), currentOffset));
				result.add(record);
				if (nextReadIdx == range.getEndOffset().getOffset()) {
					break;
				}
			}
		}
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