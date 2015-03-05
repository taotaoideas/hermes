package com.ctrip.hermes.example.demo;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.remoting.netty.NettyServer;
import com.ctrip.hermes.container.BrokerConsumerBootstrap;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.engine.scanner.Scanner;
import com.ctrip.hermes.local.LocalDevServer;

public class Bootstrap extends ComponentTestCase {

	@Test
	public void start() throws Exception {
		startBroker();
//		startLocalDevServer();
		
		startConsumers();
		OrderProducer p = new OrderProducer();

		while (true) {
			p.send();
			System.in.read();
		}
	}

	private void startConsumers() {
		List<Subscriber> subs = lookup(Scanner.class).scan();
		
		ConsumerBootstrap cb = lookup(ConsumerBootstrap.class, BrokerConsumerBootstrap.ID);
		for (Subscriber s : subs) {
			System.out.println("Found consumer class " + s.getConsumer().getClass());
			cb.startConsumer(s);
		}
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
		System.setProperty("devMode", "true");
	}
}
