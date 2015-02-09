package com.ctrip.hermes.remoting.netty;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;

import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;

public class ClientManager extends ContainerHolder implements Initializable {

	private NettyClient m_client;

	public NettyClientHandler findProducerClient(String topic) {
		return m_client.getCmdHandler();
	}

	@Override
	public void initialize() throws InitializationException {
		m_client = lookup(NettyClient.class);

		m_client.start(new Command(CommandType.HandshakeRequest));
	}

}
