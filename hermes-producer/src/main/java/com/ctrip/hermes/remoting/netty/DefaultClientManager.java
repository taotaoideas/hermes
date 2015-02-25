package com.ctrip.hermes.remoting.netty;

import org.unidal.lookup.ContainerHolder;

import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;

public class DefaultClientManager extends ContainerHolder implements ClientManager {

	private NettyClient m_client;

	private void init() {
		if (m_client == null) {
			synchronized (this) {
				if (m_client == null) {
					m_client = lookup(NettyClient.class);
					m_client.start(new Command(CommandType.HandshakeRequest));
				}
			}
		}
	}

	@Override
	public NettyClientHandler findProducerClient(String topic) {
		init();
		return m_client.getCmdHandler();
	}

	@Override
	public NettyClientHandler findConsumerClient(String topicPattern, String groupId) {
		return findProducerClient(topicPattern);
	}

}
