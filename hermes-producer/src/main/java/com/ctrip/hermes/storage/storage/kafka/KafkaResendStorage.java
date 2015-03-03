package com.ctrip.hermes.storage.storage.kafka;

import java.util.List;

import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;

import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.spi.typed.ResendStorage;
import com.ctrip.hermes.storage.storage.StorageException;

public class KafkaResendStorage extends AbstractKafkaStorage<Resend> implements ResendStorage {

	public KafkaResendStorage(String id, String partition, ProducerConfig pc, ConsumerConfig cc) {
		super(id, partition, pc, cc);
	}

	@Override
	public void append(List<Resend> payloads) throws StorageException {
		// TODO Auto-generated method stub

	}

}
