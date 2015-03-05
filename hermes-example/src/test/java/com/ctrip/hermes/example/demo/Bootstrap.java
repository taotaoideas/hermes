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

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("devMode", "true");
	}

	@Test
	public void start() throws Exception {

		startBroker();
		startLocalDevServer();

		ConsumerBootstrap cb = lookup(ConsumerBootstrap.class, BrokerConsumerBootstrap.ID);
		List<Subscriber> subs = lookup(Scanner.class).scan();
		for (Subscriber s : subs) {
			System.out.println("Found consumer class " + s.getConsumer().getClass());
			cb.startConsumer(s);
		}

		OrderProducer p = new OrderProducer();
		p.send();

		while (true) {
			System.in.read();
			p.send();
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

}
