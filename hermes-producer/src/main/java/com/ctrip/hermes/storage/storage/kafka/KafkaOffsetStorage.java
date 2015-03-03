package com.ctrip.hermes.storage.storage.kafka;

import java.util.List;

import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;

import com.ctrip.hermes.storage.spi.typed.OffsetStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.StorageException;

public class KafkaOffsetStorage extends AbstractKafkaStorage<Offset> implements OffsetStorage {

	public KafkaOffsetStorage(String id, String partition, ProducerConfig pc, ConsumerConfig cc) {
		super(id, partition, pc, cc);
	}

	@Override
	public void append(List<Offset> payloads) throws StorageException {
		// TODO Auto-generated method stub

	}

	@Override
   public Browser<Offset> createBrowser(long offset) {
	   // TODO Auto-generated method stub
	   return null;
   }

}
