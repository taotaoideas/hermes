package com.ctrip.hermes.storage.storage.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;

import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.spi.typed.ResendStorage;

public class KafkaResendStorage extends AbstractKafkaStorage<Resend> implements ResendStorage {

	public KafkaResendStorage(String id, ProducerConfig pc, ConsumerConfig cc) {
		super(id, pc, cc);
	}

}
