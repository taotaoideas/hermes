package com.ctrip.hermes.avro;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.engine.Engine;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;
import com.ctrip.hermes.producer.api.SendResult;

public class KafkaAvroTest extends ComponentTestCase {
	@Test
	public void testSimpleProducer() throws InterruptedException, ExecutionException, IOException {
		String topic = "kafka.AvroTopic";
		String group = "avroGroup";

		Producer producer = lookup(Producer.class);
		Engine engine = lookup(Engine.class);

		Subscriber s = new Subscriber(topic, group, new Consumer<AvroVisitEvent>() {

			@Override
			public void consume(List<ConsumerMessage<AvroVisitEvent>> msgs) {
				for (ConsumerMessage<AvroVisitEvent> msg : msgs) {
					AvroVisitEvent event = msg.getBody();
					System.out.println("Consumer Received: " + event);
				}
			}
		});

		System.out.println("Starting consumer...");
		engine.start(Arrays.asList(s));

		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				AvroVisitEvent event = generateEvent();
				MessageHolder holder = producer.message(topic, event);
				Future<SendResult> future = holder.send();
				future.get();
				if (future.isDone()) {
					System.out.println("Producer Sent: " + event);
				}
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
