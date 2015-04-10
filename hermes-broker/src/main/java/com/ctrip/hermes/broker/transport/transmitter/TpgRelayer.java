package com.ctrip.hermes.broker.transport.transmitter;

import com.ctrip.hermes.core.message.ConsumerMessageBatch;

/**
 * mapping to one tpg, manage multiple {@linkplain TpgChannel TpgChannel}s
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface TpgRelayer {
	void close();

	boolean isClosed();

	int availableSize();

	boolean relay(ConsumerMessageBatch batch);

	void addChannel(TpgChannel channel);

	boolean containsChannel(TpgChannel channel);
}
