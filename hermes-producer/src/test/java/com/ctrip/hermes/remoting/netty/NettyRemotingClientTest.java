package com.ctrip.hermes.remoting.netty;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;

public class NettyRemotingClientTest extends ComponentTestCase {

	@Test
	public void test() {
		NettyRemotingClient client = lookup(NettyRemotingClient.class);

		client.start(new Command(CommandType.HandshakeRequest));
	}

}
