package com.ctrip.hermes.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.remoting.netty.NettyServer;
import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.meta.entity.Connector;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.producer.Producer;

public class OneBoxTest extends ComponentTestCase {

	private Map<String, Map<String, Integer>> m_nacks = new HashMap<String, Map<String, Integer>>();

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("devMode", "false");
	}

	@Test
	public void test() throws Exception {
		startBroker();
		
		String topic = "order.new";

//		lookup(MessageQueueMonitor.class);
		Connector connector = lookup(MetaService.class).getConnector(topic);

		ConsumerBootstrap b = lookup(ConsumerBootstrap.class, connector.getType());

		Map<String, List<String>> subscribers = new HashMap<String, List<String>>();
		subscribers.put("group1", Arrays.asList("1-a", "1-b"));
		subscribers.put("group2", Arrays.asList("2-a", "2-b"));

		for (Map.Entry<String, List<String>> entry : subscribers.entrySet()) {
			String groupId = entry.getKey();
			Map<String, Integer> nacks = findNacks(groupId);
			for (String id : entry.getValue()) {
				Subscriber s = new Subscriber(topic, groupId, new MyConsumer(nacks, id));
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
					b.startConsumer(new Subscriber(topic, groupId, new MyConsumer(nacks, id)));
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

	private void send(String topic, String prefix) {
		String uuid = UUID.randomUUID().toString();
		String msg = prefix + uuid;
		System.out.println(">>> " + msg);
		Producer.getInstance().message(topic, msg).withKey(uuid).send();
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
	
	private void startBroker() {
		new Thread() {
			public void run() {
				lookup(NettyServer.class).start();
			}
		}.start();
	}
}
