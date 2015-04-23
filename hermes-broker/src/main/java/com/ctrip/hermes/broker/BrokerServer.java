package com.ctrip.hermes.broker;

import com.ctrip.hermes.broker.transport.NettyServer;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public class BrokerServer {
	public static void main(String[] args) {
		System.setProperty("devMode", "true");
		PlexusComponentLocator.lookup(NettyServer.class).start();
	}
}
