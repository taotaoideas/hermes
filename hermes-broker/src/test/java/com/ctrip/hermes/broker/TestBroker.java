package com.ctrip.hermes.broker;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.remoting.netty.NettyServer;
import com.ctrip.hermes.channel.MessageQueueMonitor;

public class TestBroker extends ComponentTestCase {

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("devMode", "true");
	}

	@Test
	public void test() throws Exception {
		lookup(MessageQueueMonitor.class);
		lookup(NettyServer.class).start();
		
		System.in.read();
	}

}
