package com.ctrip.hermes.core.transport.endpoint.event;

import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class EndpointChannelInactiveEvent extends EndpointChannelEvent {

	public EndpointChannelInactiveEvent(Object ctx, EndpointChannel channel) {
		super(ctx, channel);
	}

}
