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
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.spi.typed.MessageStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;

public class KafkaMessageStorage extends AbstractKafkaStorage<Record> implements MessageStorage {

	public KafkaMessageStorage(String id, String partition, ProducerConfig pc, ConsumerConfig cc) {
		super(id, partition, pc, cc);
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
		long nextReadIdx = 0;

		if (offset > 0) {
			nextReadIdx = offset;
		} else {
			nextReadIdx = SimpleConsumerUtil.getLastOffset(m_consumer, m_topic, m_partition_id,
			      kafka.api.OffsetRequest.EarliestTime(), m_consumer.clientId());
		}

		return new KafkaMessageBrowser(nextReadIdx);
	}

	class KafkaMessageBrowser implements Browser<Record> {

		private long m_nextReadIdx;

		public KafkaMessageBrowser(long nextReadIdx) {
			m_nextReadIdx = nextReadIdx;
		}

		@Override
		public synchronized List<Record> read(int batchSize) {
			List<Record> result = new ArrayList<>();
			int remain = batchSize;
			int numErrors = 0;
			while (remain > 0) {
				FetchRequest req = new FetchRequestBuilder().clientId(m_consumer.clientId())
				      .addFetch(m_topic, m_partition_id, m_nextReadIdx, bufferSize).build();
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

				ByteBufferMessageSet messageSet = fetchResponse.messageSet(m_topic, m_partition_id);
				if (messageSet.sizeInBytes() == 0) {
					break;
				}
				for (MessageAndOffset messageAndOffset : messageSet) {
					long currentOffset = messageAndOffset.offset();
					if (currentOffset < m_nextReadIdx) {
						System.out.println("Found an old offset: " + currentOffset + " Expecting: " + m_nextReadIdx);
						continue;
					}
					m_nextReadIdx = messageAndOffset.nextOffset();
					ByteBuffer payload = messageAndOffset.message().payload();

					byte[] bytes = new byte[payload.limit()];
					payload.get(bytes);
					Record msg = new Record();
					msg.setContent(bytes);
					msg.setOffset(new Offset(m_topic, currentOffset));
					result.add(msg);
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

	@Override
	public List<Record> read(Range range) throws StorageException {
		List<Record> result = new ArrayList<>();

		int numErrors = 0;
		long startOffset = range.startOffset().getOffset();
		long endOffset = range.endOffset().getOffset();
		while (true) {
			FetchRequest req = new FetchRequestBuilder().clientId(m_consumer.clientId())
			      .addFetch(m_topic, m_partition_id, startOffset, bufferSize).build();
			FetchResponse fetchResponse = m_consumer.fetch(req);
			if (fetchResponse.hasError()) {
				numErrors++;
				short code = fetchResponse.errorCode(m_topic, m_partition_id);
				System.out.println("Error fetching data Reason: " + code);
				if (numErrors > 5)
					break;
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					startOffset = SimpleConsumerUtil.getLastOffset(m_consumer, m_topic, m_partition_id,
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

			ByteBufferMessageSet messageSet = fetchResponse.messageSet(m_topic, m_partition_id);
			if (messageSet.sizeInBytes() == 0) {
				break;
			}
			for (MessageAndOffset messageAndOffset : messageSet) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < startOffset) {
					System.out.println("Found an old offset: " + currentOffset + " Expecting: " + startOffset);
					continue;
				}
				if (currentOffset == endOffset)
					break;
				startOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();

				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				Record msg = new Record();
				msg.setContent(bytes);
				msg.setOffset(new Offset(m_topic, currentOffset));
				result.add(msg);
			}
		}
		return result;
	}

	@Override
	public Record top() throws StorageException {
		long topOffset = SimpleConsumerUtil.getLastOffset(m_consumer, m_topic, m_partition_id,
		      kafka.api.OffsetRequest.LatestTime() - 1, m_consumer.clientId());
		FetchRequest req = new FetchRequestBuilder().clientId(m_consumer.clientId())
		      .addFetch(m_topic, m_partition_id, topOffset, bufferSize).build();
		FetchResponse fetchResponse = m_consumer.fetch(req);
		ByteBufferMessageSet messageSet = fetchResponse.messageSet(m_topic, m_partition_id);
		if (messageSet.sizeInBytes() == 0) {
			return null;
		}
		Record msg = null;
		for (MessageAndOffset messageAndOffset : messageSet) {
			long currentOffset = messageAndOffset.offset();
			if (currentOffset < topOffset) {
				System.out.println("Found an old offset: " + currentOffset + " Expecting: " + topOffset);
				continue;
			}
			topOffset = messageAndOffset.nextOffset();
			ByteBuffer payload = messageAndOffset.message().payload();

			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);
			msg = new Record();
			msg.setContent(bytes);
			msg.setOffset(new Offset(m_topic, currentOffset));
		}
		return msg;
	}
}
