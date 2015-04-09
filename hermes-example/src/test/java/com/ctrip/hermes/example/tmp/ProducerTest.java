package com.ctrip.hermes.example.tmp;

import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.broker.transport.NettyServer;
import com.ctrip.hermes.consumer.BaseConsumer;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
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

		p.message("order_new", 0L).withKey("key0").withPartition("0").addProperty("k", "v").send();
		
		List<Subscriber> subscribers = Arrays.asList(new Subscriber("order_new", "xx", new BaseConsumer<String>() {

			@Override
			protected void consume(ConsumerMessage<String> msg) {
				System.out.println(JSON.toJSONString(msg));
			}
		}));
		lookup(Engine.class).start(subscribers);

		System.in.read();
	}

}
