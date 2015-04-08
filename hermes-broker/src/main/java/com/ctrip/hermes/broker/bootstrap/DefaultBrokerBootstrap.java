package com.ctrip.hermes.broker.bootstrap;

import org.unidal.lookup.ContainerHolder;

import com.ctrip.hermes.broker.remoting.netty.NettyServer;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultBrokerBootstrap extends ContainerHolder implements BrokerBootstrap {

	@Override
	public void start() throws Exception {
		lookup(NettyServer.class).start();
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
	}

}
