package com.ctrip.hermes.container;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.producer.Producer;

public class TestKafka extends ComponentTestCase {
	@Test
	public void test() throws Exception {
		String topic = "order2.kafka";
		ConsumerBootstrap b = lookup(ConsumerBootstrap.class, BrokerConsumerBootstrap.ID);

		Subscriber s = new Subscriber(topic, "group1", new Consumer<String>() {

			private Set<String> m_nacks = new HashSet<String>();

			@Override
			public void consume(List<ConsumerMessage<String>> msgs) {
				for (ConsumerMessage<String> msg : msgs) {
					String body = msg.getBody();
					System.out.println("<<< " + body);
					if (body.startsWith("NACK-")) {
						if (!m_nacks.contains(body)) {
							msg.nack();
							m_nacks.add(body);
						} else {
							m_nacks.remove(body);
						}
					}
				}
			}
		});

		System.out.println("Starting consumer...");
		b.startConsumer(s);

		System.out.println("Starting producer...");
		send(topic, "ACK-");

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String line = in.readLine();
			String prefix = "ACK-";
			if ("q".equals(line)) {
				break;
			} else if ("n".equals(line)) {
				prefix = "NACK-";
			}

			send(topic, prefix);
		}
	}

	public void send(String topic, String prefix) {
		String msg = prefix + UUID.randomUUID().toString();
		System.out.println(">>> " + msg);
		Producer.getInstance().message(topic, msg).send();
	}
}
