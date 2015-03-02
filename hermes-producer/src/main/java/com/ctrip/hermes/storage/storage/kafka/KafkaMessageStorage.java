package com.ctrip.hermes.storage.storage.kafka;

import java.util.List;

import kafka.consumer.ConsumerConfig;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.spi.typed.MessageStorage;
import com.ctrip.hermes.storage.storage.StorageException;

public class KafkaMessageStorage extends AbstractKafkaStorage<Message> implements MessageStorage {

	public KafkaMessageStorage(String id, String partition, ProducerConfig pc, ConsumerConfig cc) {
		super(id, partition, pc, cc);
	}

	@Override
	public void append(List<Message> payloads) throws StorageException {
		for (Message payload : payloads) {
			KeyedMessage<String, Message> kafkaMsg = new KeyedMessage<>(m_topic, payload.getPartition(), payload);
			m_producer.send(kafkaMsg);
		}
	}
}
