package com.ctrip.hermes.broker;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.remoting.netty.NettyServer;

public class HermesTestServer extends ComponentTestCase {

	@Test
	public void test() throws Exception {
		lookup(NettyServer.class).start();
	}

}
