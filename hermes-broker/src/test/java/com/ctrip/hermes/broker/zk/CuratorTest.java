package com.ctrip.hermes.broker.zk;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Test;

public class CuratorTest {

	@Test
	public void testSharedCount() throws Exception {
		RetryPolicy retryPolicy = new RetryNTimes(1, 1000);
		CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", retryPolicy);
		client.start();

		int brokerCnt = 3;
		CountDownLatch latch = new CountDownLatch(brokerCnt);
		BlockingQueue<Integer> q = new LinkedBlockingQueue<Integer>();
		for (int i = 0; i < brokerCnt; i++) {
			startBroker(client, "broker" + i, q, 100, latch);
		}

		int pre = -1;
		while (true) {
			int cur = q.take();

			if (cur < 0) {
				if (latch.await(1, TimeUnit.MICROSECONDS)) {
					break;
				}
			}

			if (cur % 100 == 0) {
				System.out.println(cur);
			}

			if (pre >= 0) {
				assertEquals(pre + 10, cur);
			}
			pre = cur;
		}
	}

	private void startBroker(final CuratorFramework client, final String id, final BlockingQueue<Integer> q,
	      final int totalRound, final CountDownLatch latch) {
		new Thread() {
			public void run() {
				try {
					doRun();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			private void doRun() throws Exception {
				SharedCount count = new SharedCount(client, "/order.new/group1", 0);
				count.start();
				Random rnd = new Random(System.currentTimeMillis());
				int round = 0;

				while (round < totalRound) {
					round++;
					VersionedValue<Integer> pre = count.getVersionedValue();
					int cur = pre.getValue() + 10;
					if (count.trySetCount(pre, cur)) {
						// System.out.println(String.format("%s got %s-%s", id, pre.getValue(), cur - 1));
						q.put(cur);
					}
					Thread.sleep(rnd.nextInt(50));
				}

				latch.countDown();
				// to notify checker to stop
				q.put(-1);
				count.close();
			}
		}.start();
	}
}
