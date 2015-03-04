package com.ctrip.hermes.example.demo;

import java.util.List;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.remoting.netty.NettyServer;
import com.ctrip.hermes.container.BrokerConsumerBootstrap;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.engine.scanner.Scanner;

public class Bootstrap extends ComponentTestCase {

	@Test
	public void start() throws Exception {
		startBroker();

		ConsumerBootstrap cb = lookup(ConsumerBootstrap.class, BrokerConsumerBootstrap.ID);
		List<Subscriber> subs = lookup(Scanner.class).scan();
		for (Subscriber s : subs) {
			System.out.println("Found consumer class " + s.getConsumer().getClass());
			cb.startConsumer(s);
		}

		OrderProducer p = new OrderProducer();
		p.send();

		System.in.read();
	}

	private void startBroker() {
		lookup(NettyServer.class).start();
	}

}
