package com.ctrip.hermes.example.demo;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.transport.NettyServer;
import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.Subscribe;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;

public class Bootstrap extends ComponentTestCase {

	@Test
	public void start() throws Exception {
//		System.setProperty("devMode", "true");
//		
//		startBroker();
//		startLocalDevServer();
//
//		startConsumers();
//
//		OrderProducer p = new OrderProducer();
//
//		while (true) {
//			System.in.read();
//			p.send("order_new");
//		}
	}

	private void startConsumers() {
		List<Subscriber> subs = findSubscribers();

		Engine engine = lookup(Engine.class);
		for (Subscriber s : subs) {
			System.out.println("Found consumer class " + s.getConsumer().getClass());
		}
		engine.start(subs);
	}

	@SuppressWarnings("rawtypes")
	private List<Subscriber> findSubscribers() {
		List<Subscriber> result = new ArrayList<Subscriber>();
		List<Consumer> cs = lookupList(Consumer.class);
		for (Consumer c : cs) {
			Subscribe anno = c.getClass().getAnnotation(Subscribe.class);
			try {
				result.add(new Subscriber(anno.topicPattern(), anno.groupId(), c));
			} catch (Exception e) {

			}
		}
		return result;
	}

	private void startLocalDevServer() throws Exception {
//		LocalDevServer.getInstance().start();
	}

	private void startBroker() {
		new Thread() {
			public void run() {
				lookup(NettyServer.class).start();
			}
		}.start();
	}

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("devMode", "true");
	}
}
