package com.ctrip.hermes.example.tmp;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.remoting.netty.NettyServer;
import com.ctrip.hermes.producer.api.Producer;

public class ProducerTest extends ComponentTestCase {

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("devMode", "true");
	}

	@Test
	public void test() throws Exception {
		new Thread() {
			public void run() {
				lookup(NettyServer.class).start();
			}
		}.start();

		Thread.sleep(1000);
		Producer p = Producer.getInstance();

		p.message("order_new", 0L).withKey("key0").withPartition("0").send();
		p.message("order_new", 1L).withKey("key1").withPartition("1").withPriority().send();
		
		System.in.read();
	}

}
