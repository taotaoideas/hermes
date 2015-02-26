package com.ctrip.hermes.container;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
		ConsumerBootstrap b = lookup(ConsumerBootstrap.class, BrokerConsumerBootstrap.ID);

		Map<String, List<String>> subscribers = new HashMap<String, List<String>>();
		subscribers.put("group1", Arrays.asList("1-a", "1-b"));
		subscribers.put("group2", Arrays.asList("2-a", "2-b"));

		Map<String, Integer> nacks = new ConcurrentHashMap<String, Integer>();
		for (Map.Entry<String, List<String>> entry : subscribers.entrySet()) {
			String groupId = entry.getKey();
			for (String id : entry.getValue()) {
				Subscriber s = new Subscriber(topic, groupId, new MyConsumer(nacks, id), String.class);
				System.out.println("Starting consumer " + groupId + ":" + id);
				b.startConsumer(s);
			}

		}

		System.out.println("Starting producer...");
		send(topic, "ACK-");

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String line = in.readLine();
			String prefix = "ACK-";
			if ("q".equals(line)) {
				break;
			} else if (line.startsWith("n")) {
				prefix = "NACK-";
			}

			send(topic, prefix);
		}
	}

	private void send(String topic, String prefix) {
		String msg = prefix + UUID.randomUUID().toString();
		System.out.println(">>> " + msg);
		Producer.getInstance().message(topic, msg).send();
	}

	static class MyConsumer implements Consumer<String> {

		private Map<String, Integer> m_nacks;

		private String m_id;

		public MyConsumer(Map<String, Integer> nacks, String id) {
			m_nacks = nacks;
			m_id = id;
		}

		@Override
		public void consume(List<Message<String>> msgs) {
			for (Message<String> msg : msgs) {
				String body = msg.getBody();
				System.out.println(m_id + "<<< " + body);
				if (body.startsWith("NACK-")) {
					if (!m_nacks.containsKey(body)) {
						msg.nack();
						m_nacks.put(body, 1);
					} else {
						m_nacks.remove(body);
					}
				}
			}
		}
	}
}
