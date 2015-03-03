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

import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.spi.typed.MessageStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.StorageException;

public class KafkaMessageStorage extends AbstractKafkaStorage<Message> implements MessageStorage {

	public KafkaMessageStorage(String id, String partition, ProducerConfig pc, ConsumerConfig cc) {
		super(id, partition, pc, cc);
	}

	@Override
	public void append(List<Message> payloads) throws StorageException {
		for (Message payload : payloads) {
			KeyedMessage<String, byte[]> kafkaMsg = new KeyedMessage<>(m_topic, payload.getPartition(),
			      payload.getContent());
			m_producer.send(kafkaMsg);
		}
	}

	@Override
	public Browser<Message> createBrowser(long offset) {
		long nextReadIdx = 0;

		if (offset > 0) {
			nextReadIdx = offset;
		} else {
			nextReadIdx = SimpleConsumerUtil.getLastOffset(m_consumer, m_topic, m_partition_id,
			      kafka.api.OffsetRequest.EarliestTime(), m_consumer.clientId());
		}

		return new KafkaMessageBrowser(nextReadIdx);
	}

	class KafkaMessageBrowser implements Browser<Message> {

		private long m_nextReadIdx;

		public KafkaMessageBrowser(long nextReadIdx) {
			m_nextReadIdx = nextReadIdx;
		}

		@Override
		public synchronized List<Message> read(int batchSize) {
			List<Message> result = new ArrayList<>();
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
					// System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes));
					Message msg = new Message();
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
}
