package com.ctrip.hermes.broker;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.remoting.netty.NettyRemotingServer;

public class HermesTestServer extends ComponentTestCase {

	@Test
	public void test() {
		lookup(NettyRemotingServer.class).start();
	}
	
}
