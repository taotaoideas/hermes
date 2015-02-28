package com.ctrip.hermes.storage.storage.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;

import com.ctrip.hermes.storage.spi.typed.OffsetStorage;
import com.ctrip.hermes.storage.storage.Offset;

public class KafkaOffsetStorage extends AbstractKafkaStorage<Offset> implements OffsetStorage {

	public KafkaOffsetStorage(String id, ProducerConfig pc, ConsumerConfig cc) {
		super(id, pc, cc);
	}

}
