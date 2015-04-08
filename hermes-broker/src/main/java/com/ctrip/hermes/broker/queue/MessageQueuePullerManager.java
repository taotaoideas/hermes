package com.ctrip.hermes.broker.queue;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueuePullerManager {
	void startPuller(Tpg tpg, long correlationId, EndpointChannel channel);

}
