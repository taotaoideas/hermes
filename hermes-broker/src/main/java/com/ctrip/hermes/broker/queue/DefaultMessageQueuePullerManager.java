package com.ctrip.hermes.broker.queue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.queue.MessageQueuePuller.ShutdownListener;
import com.ctrip.hermes.broker.transport.transmitter.TpgRelayer;
import com.ctrip.hermes.core.bo.Tpg;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MessageQueuePullerManager.class)
public class DefaultMessageQueuePullerManager implements MessageQueuePullerManager {

	private ConcurrentMap<Tpg, MessageQueuePuller> m_pullers = new ConcurrentHashMap<>();

	@Override
	public void startPuller(final Tpg tpg, TpgRelayer relayer) {
		m_pullers.putIfAbsent(tpg, new DefaultMessageQueuePuller(tpg, relayer, new ShutdownListener() {

			@Override
			public void onShutdown() {
				m_pullers.remove(tpg);
			}
		}));
		m_pullers.get(tpg).start();
	}

}
