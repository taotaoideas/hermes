package com.ctrip.hermes.channel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultEndpointChannelManager implements EndpointChannelManager {
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
						EndpointChannel channel = new RemoteEndpointChannel(endpoint.getHost(), endpoint.getPort());
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
