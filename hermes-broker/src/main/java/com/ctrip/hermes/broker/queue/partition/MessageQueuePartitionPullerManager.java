package com.ctrip.hermes.broker.queue.partition;

import com.ctrip.hermes.broker.transport.transmitter.TpgRelayer;
import com.ctrip.hermes.core.bo.Tpg;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueuePartitionPullerManager {
	void startPuller(Tpg tpg, TpgRelayer relayer);

}
