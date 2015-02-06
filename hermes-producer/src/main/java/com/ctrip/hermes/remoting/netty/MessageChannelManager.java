package com.ctrip.hermes.remoting.netty;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;

import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;

public class MessageChannelManager extends ContainerHolder implements Initializable {

	private NettyClient m_client;

	private ProducerChannel m_producerChannel;

	public ProducerChannel findProducerChannel(String topic) {
		return m_producerChannel;
	}

	@Override
	public void initialize() throws InitializationException {
		m_client = lookup(NettyClient.class);

		m_client.start(new Command(CommandType.HandshakeRequest));

		m_producerChannel = new ProducerChannel(m_client.getCmdHandler());
	}

	public void registerConsumerChannel(ConsumerChannel channel) {
		// TODO Auto-generated method stub

	}

}
