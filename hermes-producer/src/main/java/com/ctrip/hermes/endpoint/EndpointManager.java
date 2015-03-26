package com.ctrip.hermes.endpoint;

import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EndpointManager {

	Endpoint getEndpoint(String topic, int partitionNo);

}
