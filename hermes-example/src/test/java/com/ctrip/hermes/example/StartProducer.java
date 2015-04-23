package com.ctrip.hermes.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.UUID;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.producer.api.Producer;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class StartProducer extends ComponentTestCase {

	@Test
	public void test() throws Exception {
		String topic = "order_new";
		System.out.println(String.format("Starting producer(topic=%s)...", topic));

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String line = in.readLine();
			if ("q".equals(line)) {
				break;
			} else {
				send(topic);
			}

		}
	}

	private void send(String topic) throws Exception {
		String uuid = UUID.randomUUID().toString();
		String msg = uuid;
		Random random = new Random();

		boolean priority = random.nextBoolean();
		msg += priority ? " priority" : " non-priority";
		System.out.println(">>> " + msg);
		if (priority) {
			Producer.getInstance().message(topic, msg).withKey(uuid).withPriority().send();
		} else {
			Producer.getInstance().message(topic, msg).withKey(uuid).send();
		}

	}
}
