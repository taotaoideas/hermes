package com.ctrip.hermes.container.kakfa;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.container.KafkaConsumerBootstrap;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.producer.Producer;
import com.ctrip.hermes.producer.Producer.Holder;

public class WildcardTopicsTest extends ComponentTestCase {

	@Test
	public void testWildcardTopics() throws IOException {
		String topicPattern = "kafka.SimpleTopic.*";
		String sendTopic1 = "kafka.SimpleTopic1";
		String sendTopic2 = "kafka.SimpleTopic2";
		String group = "group1";

		Producer producer = lookup(Producer.class);

		ConsumerBootstrap b = lookup(ConsumerBootstrap.class, KafkaConsumerBootstrap.ID);

		Subscriber s = new Subscriber(topicPattern, group, new Consumer<VisitEvent>() {

			@Override
			public void consume(List<Message<VisitEvent>> msgs) {
				for (Message<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println(String.format("Receive from %s: %s", msg.getTopic(), event));
				}
			}
		});

		System.out.println("Starting consumer...");
		b.startConsumer(s);

		Random random = new Random();
		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				VisitEvent event = ProduerTest.generateEvent();
				String topic = random.nextBoolean() ? sendTopic1 : sendTopic2;
				Holder holder = producer.message(topic, event);
				holder.send();
				System.out.println(String.format("Sent to %s: %s", topic, event));
			}
		}
	}
}
