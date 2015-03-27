package com.ctrip.hermes.container.kakfa;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.container.KafkaConsumerBootstrap;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.producer.Producer;
import com.ctrip.hermes.producer.Producer.MessageHolder;

public class PartitionTest extends ComponentTestCase {

	@Test
	public void testTwoPartitionOneConsumer() throws IOException {
		String topicPattern = "kafka.SimpleTopic";
		String group = "group2";

		Producer producer = lookup(Producer.class);

		ConsumerBootstrap b = lookup(ConsumerBootstrap.class, KafkaConsumerBootstrap.ID);

		Subscriber s = new Subscriber(topicPattern, group, new Consumer<VisitEvent>() {

			@Override
			public void consume(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println(String.format("Receive from %s %s: %s", msg.getTopic(), msg.getPartition(), event));
				}
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
				MessageHolder holder = producer.message(topicPattern, event);
				holder.send();
				System.out.println(String.format("Sent to %s: %s", topicPattern, event));
			}
		}
	}

	@Test
	public void testTwoPartitionTwoConsumer() throws IOException {
		String topicPattern = "kafka.SimpleTopic";
		String group = "group2";

		Producer producer = lookup(Producer.class);

		ConsumerBootstrap b = lookup(ConsumerBootstrap.class, KafkaConsumerBootstrap.ID);

		Subscriber s1 = new Subscriber(topicPattern, group, new Consumer<VisitEvent>() {

			@Override
			public void consume(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println(String.format("Consumer1 Receive from %s %s: %s", msg.getTopic(), msg.getPartition(),
					      event));
				}
			}
		});

		System.out.println("Starting consumer1...");
		b.startConsumer(s1);

		Subscriber s2 = new Subscriber(topicPattern, group, new Consumer<VisitEvent>() {

			@Override
			public void consume(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println(String.format("Consumer2 Receive from %s %s: %s", msg.getTopic(), msg.getPartition(),
					      event));
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
				MessageHolder holder = producer.message(topicPattern, event);
				holder.send();
				System.out.println(String.format("Sent to %s: %s", topicPattern, event));
			}
		}
	}
}
