package com.ctrip.hermes.kafka.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class PartitionTest {

	@Test
	public void testTwoPartitionOneConsumer() throws IOException {
		String topicPattern = "kafka.SimpleTopic";
		String group = "group2";

		Producer producer = Producer.getInstance();

		Engine engine = Engine.getInstance();

		Subscriber s = new Subscriber(topicPattern, group, new Consumer<VisitEvent>() {

			@Override
			public void consume(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println(String.format("Receive from %s: %s", msg.getTopic(), event));
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

				VisitEvent event = ProducerTest.generateEvent();
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

		Producer producer = Producer.getInstance();

		Engine engine = Engine.getInstance();

		Subscriber s1 = new Subscriber(topicPattern, group, new Consumer<VisitEvent>() {

			@Override
			public void consume(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println(String.format("Consumer1 Receive from %s: %s", msg.getTopic(), event));
				}
			}
		});

		System.out.println("Starting consumer1...");
		engine.start(Arrays.asList(s1));

		Subscriber s2 = new Subscriber(topicPattern, group, new Consumer<VisitEvent>() {

			@Override
			public void consume(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println(String.format("Consumer2 Receive from %s: %s", msg.getTopic(), event));
				}
			}
		});

		System.out.println("Starting consumer2...");
		engine.start(Arrays.asList(s2));

		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				VisitEvent event = ProducerTest.generateEvent();
				MessageHolder holder = producer.message(topicPattern, event);
				holder.send();
				System.out.println(String.format("Sent to %s: %s", topicPattern, event));
			}
		}
	}
}
