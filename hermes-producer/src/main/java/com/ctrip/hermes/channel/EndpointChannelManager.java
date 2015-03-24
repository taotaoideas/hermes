package com.ctrip.hermes.channel;

import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EndpointChannelManager {

	/**
	 * @param endpoint
	 * @return
	 */
   EndpointChannel getChannel(Endpoint endpoint);

}
