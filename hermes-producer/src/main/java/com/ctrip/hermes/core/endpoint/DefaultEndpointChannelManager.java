package com.ctrip.hermes.core.endpoint;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultEndpointChannelManager implements EndpointChannelManager {
	
	@Inject
	private CommandProcessorManager m_cmdProcessorManager;
	
	private ConcurrentMap<Endpoint, EndpointChannel> channels = new ConcurrentHashMap<>();

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.channel.EndpointChannelManager#getChannel(com.ctrip.hermes.meta.entity.Endpoint)
	 */
	@Override
	public EndpointChannel getChannel(Endpoint endpoint) {
		switch (endpoint.getType()) {
		case Endpoint.BROKER:
			if (!channels.containsKey(endpoint)) {
				synchronized (channels) {
					if (!channels.containsKey(endpoint)) {
						EndpointChannel channel = new NettyClientEndpointChannel(endpoint.getHost(), endpoint.getPort(), m_cmdProcessorManager);
						channel.start();
						channels.put(endpoint, channel);
					}
				}
			}
			return channels.get(endpoint);

		default:
			throw new UnsupportedOperationException(String.format("unknow endpoint type: %s", endpoint.getType()));
		}
	}
}
