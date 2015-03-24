package com.ctrip.hermes.message.internal;

import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EndpointManager {

	/**
	 * @param topic
	 * @param partition
	 * @return
	 */
	Endpoint getEndpoint(String topic, String partition);

}
