package com.ctrip.hermes.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;
import org.unidal.lookup.LookupException;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;
import com.ctrip.hermes.consumer.BaseConsumer;
import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;

public class OneBoxTest extends ComponentTestCase {

	private Map<String, Map<String, Integer>> m_nacks = new HashMap<String, Map<String, Integer>>();

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("devMode", "false");
	}

	@Test
	public void testProduce() throws Exception {
		startBroker();
		Producer p = Producer.getInstance();

		p.message("order_new", 1233213423L).withKey("key").withPartition("0").withPriority().send();

		System.in.read();
	}

	@Test
	public void testConsumer() throws Exception {
		startBroker();

		Thread.sleep(2000);
		Engine engine = lookup(Engine.class);

		Subscriber s = new Subscriber("order_new", "sdf", new BaseConsumer<Long>() {

			@Override
			protected void consume(ConsumerMessage<Long> msg) {
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>Received: " + msg.getBody());
			}
		});
		engine.start(Arrays.asList(s));

		System.in.read();
	}

	@Test
	public void test() throws Exception {
		startBroker();

		String topic = "order_new";

		Engine engine = lookup(Engine.class);

		Map<String, List<String>> subscribers = new HashMap<String, List<String>>();
		subscribers.put("group1", Arrays.asList("1-a", "1-b"));
		subscribers.put("group2", Arrays.asList("2-a", "2-b"));
		subscribers.put("group3", Arrays.asList("3-a", "3-b", "3-c"));

		for (Map.Entry<String, List<String>> entry : subscribers.entrySet()) {
			String groupId = entry.getKey();
			Map<String, Integer> nacks = findNacks(groupId);
			for (String id : entry.getValue()) {
				Subscriber s = new Subscriber(topic, groupId, new MyConsumer(nacks, id));
				System.out.println("Starting consumer " + groupId + ":" + id);
				engine.start(Arrays.asList(s));
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
				int nackCnt = 1;
				try {
					nackCnt = Integer.parseInt(line.substring(1).trim());
				} catch (Exception e) {
				}

				prefix = "NACK-" + nackCnt + "-";

				send(topic, prefix);
			} else if (line.startsWith("c")) {
				String[] parts = line.split(" ");
				if (parts.length == 3) {
					String groupId = parts[1];
					String id = parts[2];
					Map<String, Integer> nacks = findNacks(groupId);
					System.out.println(String.format("Starting consumer with groupId %s and id %s", groupId, id));
					engine.start(Arrays.asList((new Subscriber(topic, groupId, new MyConsumer(nacks, id)))));
				}
			} else {
				send(topic, prefix);
			}

		}
	}

	private Map<String, Integer> findNacks(String groupId) {
		if (!m_nacks.containsKey(groupId)) {
			m_nacks.put(groupId, new ConcurrentHashMap<String, Integer>());
		}
		return m_nacks.get(groupId);
	}

	private void send(String topic, String prefix) throws Exception {
		String uuid = UUID.randomUUID().toString();
		String msg = prefix + uuid;
		System.out.println(">>> " + msg);
		Future<SendResult> future = Producer.getInstance().message(topic, msg).withKey(uuid).send();

		future.get();

	}

	static class MyConsumer implements Consumer<String> {

		private Map<String, Integer> m_nacks;

		private String m_id;

		public MyConsumer(Map<String, Integer> nacks, String id) {
			m_nacks = nacks;
			m_id = id;
		}

		@Override
		public void consume(List<ConsumerMessage<String>> msgs) {
			for (ConsumerMessage<String> msg : msgs) {
				String body = msg.getBody();
				System.out.println(m_id + "<<< " + body);
				if (body.startsWith("NACK-")) {
					int totalNackCnt = Integer.parseInt(body.substring(5, body.indexOf("-", 5)));

					if (!m_nacks.containsKey(body)) {
						m_nacks.put(body, 1);
						msg.nack();
					} else {
						int curNackCnt = m_nacks.get(body);
						if (curNackCnt < totalNackCnt) {
							m_nacks.put(body, curNackCnt + 1);
							msg.nack();
						} else {
							m_nacks.remove(body);
						}
					}
				}
			}
		}
	}

	private void startBroker() throws Exception {
		new Thread() {
			public void run() {

				try {
					lookup(BrokerBootstrap.class).start();
				} catch (LookupException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			};
		}.start();

		Thread.sleep(2000);
	}
}
