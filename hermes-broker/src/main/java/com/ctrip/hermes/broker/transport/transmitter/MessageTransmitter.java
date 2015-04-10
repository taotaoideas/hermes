package com.ctrip.hermes.broker.transport.transmitter;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageTransmitter {

	TpgRelayer registerDestination(Tpg tpg, long correlationId, EndpointChannel channel, int window);

}
