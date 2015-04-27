package com.ctrip.hermes.core.transport.endpoint;

import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EndpointChannelManager {

	EndpointChannel getChannel(Endpoint endpoint, EndpointChannelEventListener... listeners);

}
