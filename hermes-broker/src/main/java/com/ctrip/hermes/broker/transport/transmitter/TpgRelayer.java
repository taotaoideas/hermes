package com.ctrip.hermes.broker.transport.transmitter;

import java.util.List;

import com.ctrip.hermes.core.message.TppConsumerMessageBatch;

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

	boolean relay(List<TppConsumerMessageBatch> batches);

	void addChannel(TpgChannel channel);

	boolean containsChannel(TpgChannel channel);
}
