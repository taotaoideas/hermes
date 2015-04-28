package com.ctrip.hermes.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import com.ctrip.hermes.consumer.BaseConsumer;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class OneBoxTest {

	@Test
	public void example() throws IOException {
		String topic = "kafka.OneBox";
		String group = "group" + RandomStringUtils.randomAlphabetic(5);

		Producer producer = Producer.getInstance();

		Engine engine = Engine.getInstance();

		Subscriber s = new Subscriber(topic, group, new BaseConsumer<String>() {

			@Override
			protected void consume(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				System.out.println("Receive: " + body);
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

				String proMsg = RandomStringUtils.randomAlphanumeric(10) + System.currentTimeMillis();
				MessageHolder holder = producer.message(topic, proMsg);
				holder.send();
				System.out.println("Sent: " + proMsg);
			}
		}
	}
}
