package com.ctrip.hermes.container;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.UUID;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.Message;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.producer.Producer;

public class IntegrationTest extends ComponentTestCase {

	@Test
	public void test() throws Exception {
		String topic = "order.new";
		ConsumerBootstrap b = lookup(ConsumerBootstrap.class);

		Subscriber s = new Subscriber(topic, "group1", new Consumer<String>() {

			@Override
			public void consume(List<Message<String>> msgs) {
				for (Message<String> msg : msgs) {
					System.out.println("<<< " + msg.getBody());
				}
			}
		});

		System.out.println("Starting consumer...");
		b.startConsumer(s);

		System.out.println("Starting producer...");
		send(topic);

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String line = in.readLine();
			if ("q".equals(line)) {
				break;
			}

			send(topic);
		}
	}

	private void send(String topic) {
		String msg = UUID.randomUUID().toString();
		System.out.println(">>> " + msg);
		Producer.getInstance().message(topic, msg).send();
	}

}
