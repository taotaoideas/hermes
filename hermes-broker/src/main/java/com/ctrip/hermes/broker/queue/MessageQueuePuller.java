package com.ctrip.hermes.broker.queue;

import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueuePuller {
	public void start();

	public void addEndpoint(long correlationId, EndpointChannel channel);
}
