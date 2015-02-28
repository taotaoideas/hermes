package com.ctrip.hermes.storage.storage.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;

import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.spi.typed.MessageStorage;

public class KafkaMessageStorage extends AbstractKafkaStorage<Message> implements MessageStorage {

	public KafkaMessageStorage(String id, ProducerConfig pc, ConsumerConfig cc) {
		super(id, pc, cc);
	}

}
