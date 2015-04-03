package com.ctrip.hermes.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class WildcardTopicsTest extends ComponentTestCase {

	@Test
	public void testWildcardTopics() throws IOException {
		String topicPattern = "kafka.SimpleTopic.*";
		String sendTopic1 = "kafka.SimpleTopic1";
		String sendTopic2 = "kafka.SimpleTopic2";
		String group = "group1";

		Producer producer = lookup(Producer.class);

		Engine engine = lookup(Engine.class);

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

		Random random = new Random();
		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				VisitEvent event = ProduerTest.generateEvent();
				String topic = random.nextBoolean() ? sendTopic1 : sendTopic2;
				MessageHolder holder = producer.message(topic, event);
				holder.send();
				System.out.println(String.format("Sent to %s: %s", topic, event));
			}
		}
	}
}
