package com.ctrip.hermes.storage.storage.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndOffset;
import kafka.producer.ProducerConfig;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.storage.spi.Storage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;

public abstract class AbstractKafkaStorage<T> implements Storage<T> {

	protected String m_topic;

	protected Producer<String, T> m_producer;

	protected String m_partition;

	protected int m_partition_id;

	protected SimpleConsumer m_consumer;

	private int bufferSize = 100;

	private List<Pair<String, Integer>> brokers = new ArrayList<>();

	public AbstractKafkaStorage(String id, String partition, ProducerConfig pc, ConsumerConfig cc) {
		m_topic = id;
		m_partition = partition;
		m_producer = new Producer<>(pc);
		String[] brokerList = cc.props().getString("metadata.broker.list").split(",");
		for (String broker : brokerList) {
			brokers.add(new Pair<>(broker.split(":")[0], Integer.parseInt(broker.split(":")[1])));
		}

		TopicMetadata topicMetadata = SimpleConsumerUtil
		      .getTopicMetadata(brokers.get(0).getKey(), brokers.get(0).getValue(), m_topic).topicsMetadata().get(0);

		m_partition_id = partition.hashCode() % topicMetadata.partitionsMetadata().size();
		PartitionMetadata metadata = SimpleConsumerUtil.findLeader(brokers, m_topic, m_partition_id);
		if (metadata == null) {
			System.out.println("Can't find metadata for Topic and Partition. Exiting");
			return;
		}
		if (metadata.leader() == null) {
			System.out.println("Can't find Leader for Topic and Partition. Exiting");
			return;
		}
		String leadBroker = metadata.leader().host();
		String clientName = "Client_" + m_topic + "_" + m_partition_id;

		m_consumer = new SimpleConsumer(leadBroker, metadata.leader().port(), bufferSize, 64 * 1024, clientName);
	}

	@Override
	public Browser<T> createBrowser(long offset) {
		long nextReadIdx = 0;

		if (offset > 0) {
			nextReadIdx = offset;
		} else {
			nextReadIdx = SimpleConsumerUtil.getLastOffset(m_consumer, m_topic, m_partition_id,
			      kafka.api.OffsetRequest.EarliestTime(), m_consumer.clientId());
		}

		return new KafkaBrowser(nextReadIdx);
	}

	@Override
	public List<T> read(Range range) throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T top() throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		return this.m_topic;
	}

	class KafkaBrowser implements Browser<T> {

		private long m_nextReadIdx;

		public KafkaBrowser(long nextReadIdx) {
			m_nextReadIdx = nextReadIdx;
		}

		@Override
		public synchronized List<T> read(int batchSize) {
			List<T> result = new ArrayList<T>();
			int remain = batchSize;
			int numErrors = 0;
			while (remain > 0) {
				FetchRequest req = new FetchRequestBuilder().clientId(m_consumer.clientId())
				      .addFetch(m_topic, m_partition_id, m_nextReadIdx, batchSize).build();
				FetchResponse fetchResponse = m_consumer.fetch(req);
				if (fetchResponse.hasError()) {
					numErrors++;
					short code = fetchResponse.errorCode(m_topic, m_partition_id);
					System.out.println("Error fetching data Reason: " + code);
					if (numErrors > 5)
						break;
					if (code == ErrorMapping.OffsetOutOfRangeCode()) {
						m_nextReadIdx = SimpleConsumerUtil.getLastOffset(m_consumer, m_topic, m_partition_id,
						      kafka.api.OffsetRequest.LatestTime(), m_consumer.clientId());
						continue;
					}
					m_consumer.close();
					PartitionMetadata metadata = SimpleConsumerUtil.findLeader(brokers, m_topic, m_partition_id);
					String leadBroker = metadata.leader().host();
					String clientName = "Client_" + m_topic + "_" + m_partition_id;

					m_consumer = new SimpleConsumer(leadBroker, metadata.leader().port(), bufferSize, 64 * 1024, clientName);
					continue;
				}
				numErrors = 0;

				for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(m_topic, m_partition_id)) {
					long currentOffset = messageAndOffset.offset();
					if (currentOffset < m_nextReadIdx) {
						System.out.println("Found an old offset: " + currentOffset + " Expecting: " + m_nextReadIdx);
						continue;
					}
					m_nextReadIdx = messageAndOffset.nextOffset();
					ByteBuffer payload = messageAndOffset.message().payload();

					byte[] bytes = new byte[payload.limit()];
					payload.get(bytes);
					System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes));
					//TODO convert bytes to Message
					remain--;
				}
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
}
