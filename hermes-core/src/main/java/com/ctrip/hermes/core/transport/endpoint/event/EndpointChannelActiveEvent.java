package com.ctrip.hermes.core.transport.endpoint.event;

import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class EndpointChannelActiveEvent extends EndpointChannelEvent {

	public EndpointChannelActiveEvent(Object ctx, EndpointChannel channel) {
		super(ctx, channel);
	}

}
