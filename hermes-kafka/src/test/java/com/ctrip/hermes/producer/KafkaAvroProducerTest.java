package com.ctrip.hermes.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.avro.AvroVisitEvent;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;
import com.ctrip.hermes.producer.api.SendResult;

public class KafkaAvroProducerTest extends ComponentTestCase {
	@Test
	public void testSimpleProducer() throws InterruptedException, ExecutionException {
		String topic = "kafka.AvroTopic";

		Producer producer = lookup(Producer.class);

		List<Future<SendResult>> result = new ArrayList<>();
		for (int i = 0; i < 2; i++) {
			AvroVisitEvent event = generateEvent();
			MessageHolder holder = producer.message(topic, event);
			Future<SendResult> send = holder.send();
			result.add(send);
		}

		for (Future<SendResult> future : result) {
			future.get();
			if (future.isDone()) {
				System.out.println("DONE: " + future.toString());
			}
		}
	}

	static AtomicLong counter = new AtomicLong();

	static AvroVisitEvent generateEvent() {
		Random random = new Random(System.currentTimeMillis());
		AvroVisitEvent event = AvroVisitEvent.newBuilder().setIp("192.168.0." + random.nextInt(255))
		      .setTz(System.currentTimeMillis()).setUrl("www.ctrip.com/" + counter.incrementAndGet()).build();
		return event;
	}
}
