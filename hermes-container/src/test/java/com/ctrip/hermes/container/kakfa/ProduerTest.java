package com.ctrip.hermes.container.kakfa;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.producer.Producer;
import com.ctrip.hermes.producer.Producer.MessageHolder;

public class ProduerTest extends ComponentTestCase {

	@Test
	public void testSimpleProducer() throws InterruptedException, ExecutionException {
		String topic = "kafka.SimpleTopic";

		Producer producer = lookup(Producer.class);

		List<Future<SendResult>> result = new ArrayList<>();
		for (int i = 0; i < 2; i++) {
			VisitEvent event = generateEvent();
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

	static VisitEvent generateEvent() {
		Random random = new Random(System.currentTimeMillis());
		VisitEvent event = new VisitEvent();
		event.ip = "192.168.0." + random.nextInt(255);
		event.tz = new Date();
		event.url = "www.ctrip.com/" + counter.incrementAndGet();
		return event;
	}
}
