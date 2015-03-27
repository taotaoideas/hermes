package com.ctrip.hermes.container.kakfa;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.BaseConsumer;
import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.container.KafkaConsumerBootstrap;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.producer.Producer;
import com.ctrip.hermes.producer.Producer.MessageHolder;

public class ConsumerTest extends ComponentTestCase {

	@Test
	public void testOneConsumerOneGroup() throws IOException {
		String topic = "kafka.SimpleTopic";
		String group = "group1";

		Producer producer = lookup(Producer.class);

		ConsumerBootstrap b = lookup(ConsumerBootstrap.class, KafkaConsumerBootstrap.ID);

		Subscriber s = new Subscriber(topic, group, new BaseConsumer<VisitEvent>() {

			@Override
			protected void consume(ConsumerMessage<VisitEvent> msg) {
				VisitEvent event = msg.getBody();
				System.out.println("Receive: " + event);
			}
		});

		System.out.println("Starting consumer...");
		b.startConsumer(s);

		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				VisitEvent event = ProduerTest.generateEvent();
				MessageHolder holder = producer.message(topic, event);
				holder.send();
				System.out.println("Sent: " + event);
			}
		}

	}

	@Test
	public void testTwoConsumerOneGroup() throws IOException {
		String topic = "kafka.SimpleTopic";
		String group = "group1";

		Producer producer = lookup(Producer.class);

		ConsumerBootstrap b = lookup(ConsumerBootstrap.class, KafkaConsumerBootstrap.ID);

		Subscriber s1 = new Subscriber(topic, group, new Consumer<VisitEvent>() {

			@Override
			public void consume(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println("Consumer1 Receive: " + event);
				}
			}
		});

		System.out.println("Starting consumer1...");
		b.startConsumer(s1);

		Subscriber s2 = new Subscriber(topic, group, new Consumer<VisitEvent>() {

			@Override
			public void consume(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println("Consumer2 Receive: " + event);
				}
			}
		});

		System.out.println("Starting consumer2...");
		b.startConsumer(s2);

		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				VisitEvent event = ProduerTest.generateEvent();
				MessageHolder holder = producer.message(topic, event);
				holder.send();
				System.out.println("Sent: " + event);
			}
		}
	}

	@Test
	public void testTwoConsumerTwoGroup() throws IOException {
		String topic = "kafka.SimpleTopic";
		String group1 = "group1";
		String group2 = "group2";

		Producer producer = lookup(Producer.class);

		ConsumerBootstrap b = lookup(ConsumerBootstrap.class, KafkaConsumerBootstrap.ID);

		Subscriber s1 = new Subscriber(topic, group1, new Consumer<VisitEvent>() {

			@Override
			public void consume(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println("Consumer1 Receive: " + event);
				}
			}
		});

		System.out.println("Starting consumer1...");
		b.startConsumer(s1);

		Subscriber s2 = new Subscriber(topic, group2, new Consumer<VisitEvent>() {

			@Override
			public void consume(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println("Consumer2 Receive: " + event);
				}
			}
		});

		System.out.println("Starting consumer2...");
		b.startConsumer(s2);

		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				VisitEvent event = ProduerTest.generateEvent();
				MessageHolder holder = producer.message(topic, event);
				holder.send();
				System.out.println("Sent: " + event);
			}
		}
	}
}
