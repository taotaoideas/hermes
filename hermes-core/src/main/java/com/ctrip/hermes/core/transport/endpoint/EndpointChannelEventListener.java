package com.ctrip.hermes.core.transport.endpoint;

import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelEvent;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EndpointChannelEventListener {
	public void onEvent(EndpointChannelEvent event);
}
