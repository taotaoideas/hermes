package com.ctrip.hermes.broker.queue;

import com.ctrip.hermes.broker.transport.transmitter.TpgRelay;
import com.ctrip.hermes.core.bo.Tpg;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueuePullerManager {
	void startPuller(Tpg tpg, TpgRelay relay);

}
