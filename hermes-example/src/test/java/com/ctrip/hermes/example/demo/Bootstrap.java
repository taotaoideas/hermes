package com.ctrip.hermes.example.demo;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.remoting.netty.NettyServer;
import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.Subscribe;
import com.ctrip.hermes.container.BrokerConsumerBootstrap;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.local.LocalDevServer;

public class Bootstrap extends ComponentTestCase {

	@Test
	public void start() throws Exception {
		System.setProperty("devMode", "true");
		
		startBroker();
		startLocalDevServer();

		startConsumers();
		
		OrderProducer p = new OrderProducer();
		p.send("order.new");
		p.send("order.update");

		while (true) {
			System.in.read();
			p.send("order.new");
		}
	}

	private void startConsumers() {
		List<Subscriber> subs = findSubscribers();

		ConsumerBootstrap cb = lookup(ConsumerBootstrap.class, BrokerConsumerBootstrap.ID);
		for (Subscriber s : subs) {
			System.out.println("Found consumer class " + s.getConsumer().getClass());
			cb.startConsumer(s);
		}
	}

	@SuppressWarnings("rawtypes")
	private List<Subscriber> findSubscribers() {
		List<Subscriber> result = new ArrayList<Subscriber>();
		List<Consumer> cs = lookupList(Consumer.class);
		for (Consumer c : cs) {
			Subscribe anno = c.getClass().getAnnotation(Subscribe.class);
			try {
				result.add(new Subscriber(anno.topicPattern(), anno.groupId(), c, anno.messageClass()));
			} catch (Exception e) {

			}
		}
		return result;
	}

	private void startLocalDevServer() throws Exception {
		LocalDevServer.getInstance().start();
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
		System.setProperty("devMode", "false");
	}
}
