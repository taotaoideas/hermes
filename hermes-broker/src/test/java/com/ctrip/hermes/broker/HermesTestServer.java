package com.ctrip.hermes.broker;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.remoting.netty.NettyServer;

public class HermesTestServer extends ComponentTestCase {

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("devMode", "true");
	}

	@Test
	public void test() throws Exception {
		lookup(NettyServer.class).start();
	}

}
