package com.ctrip.hermes.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.helper.Threads;
import org.unidal.lookup.ComponentTestCase;
import org.unidal.lookup.LookupException;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;
import com.ctrip.hermes.consumer.BaseConsumer;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.dianping.cat.Cat;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

public class OneBoxTest extends ComponentTestCase {

	private Map<String, Map<String, Integer>> m_nacks = new HashMap<String, Map<String, Integer>>();

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("devMode", "true");
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
		final AtomicLong counter = new AtomicLong(0);

		Subscriber s = new Subscriber("order_new", "sdf", new BaseConsumer<Long>() {

			@Override
			protected void consume(ConsumerMessage<Long> msg) {
				counter.incrementAndGet();
				// System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>Received: " + msg.getBody());
			}
		});
		engine.start(Arrays.asList(s));

		System.in.read();
	}

	@Test
	public void testProducePerformance() throws Exception {
		startBroker();

		final int times = 20000;
		int threadCount = 2;
		final String topic = "order_new";
		final CountDownLatch latch = new CountDownLatch(times * threadCount);
		Thread.sleep(2000);
		Producer p = Producer.getInstance();

		p.message(topic, 1233213423L).withKey("key").withPartition("0").withPriority().send();
		p.message(topic, 1233213423L).withKey("key").withPartition("0").withPriority().send();
		Thread.sleep(1000);

		long start = System.currentTimeMillis();

		for (int i = 0; i < threadCount; i++) {
			Thread t = new Thread() {
				public void run() {
					producePerformance(times, topic, latch);
				}
			};
			t.start();
		}

		// latch.await(30, TimeUnit.SECONDS);
		latch.await();

		long progressTime = System.currentTimeMillis() - start;
		System.out.println(String.format("%d Threads produce %d msgs spends %d ms, QPS: %.2f msg/s", threadCount, times
		      * threadCount, progressTime, (float) (times * threadCount) / (progressTime / 1000f)));

		System.in.read();
	}

	private void producePerformance(int times, String topic, final CountDownLatch latch) {
		Random random = new Random();

		for (int i = 0; i < times; i++) {
			String uuid = UUID.randomUUID().toString();
			String msg = uuid;

			boolean priority = random.nextBoolean();
			SettableFuture<SendResult> future;
			if (priority) {
				future = (SettableFuture<SendResult>) Producer.getInstance().message(topic, msg + " priority")
				      .withKey(uuid).withPriority().send();
			} else {
				future = (SettableFuture<SendResult>) Producer.getInstance().message(topic, msg + " non-priority")
				      .withKey(uuid).send();
			}

			future.addListener(new Runnable() {

				@Override
				public void run() {
					latch.countDown();
				}
			}, MoreExecutors.sameThreadExecutor());
		}
	}

	@Test
	public void test() throws Exception {
		Cat.initialize("cat.fws.qa.nt.ctripcorp.com");
		startBroker();

		String topic = "order_new";

		Engine engine = lookup(Engine.class);

		Map<String, List<String>> subscribers = new HashMap<String, List<String>>();
		subscribers.put("group2", Arrays.asList("1-a"));
		// subscribers.put("group1", Arrays.asList("1-a", "1-b"));
		// subscribers.put("group2", Arrays.asList("2-a", "2-b"));
		// subscribers.put("group3", Arrays.asList("3-a", "3-b", "3-c"));

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
		// send(topic, "ACK-");

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
		Random random = new Random();

		boolean priority = random.nextBoolean();
		Future<SendResult> future;
		if (priority) {
			future = Producer.getInstance().message(topic, msg + " priority").withKey(uuid).withPriority().send();
		} else {
			future = Producer.getInstance().message(topic, msg + " non-priority").withKey(uuid).send();
		}

		future.get();

	}

	static class MyConsumer extends BaseConsumer<String> {

		private Map<String, Integer> m_nacks;

		private String m_id;

		public MyConsumer(Map<String, Integer> nacks, String id) {
			m_nacks = nacks;
			m_id = id;
		}

		@Override
		public void consume(ConsumerMessage<String> msg) {
			String body = msg.getBody();
			System.out.println(m_id + "<<< " + body);

			// TODO
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
						msg.ack();
					}
				}
			} else {
				msg.ack();
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
