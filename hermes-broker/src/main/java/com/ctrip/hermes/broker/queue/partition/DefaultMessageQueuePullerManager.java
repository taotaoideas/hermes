package com.ctrip.hermes.broker.queue.partition;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.broker.queue.partition.MessageQueuePartitionPuller.ShutdownListener;
import com.ctrip.hermes.broker.transport.transmitter.TpgRelayer;
import com.ctrip.hermes.core.bo.Tpg;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MessageQueuePartitionPullerManager.class)
public class DefaultMessageQueuePullerManager implements MessageQueuePartitionPullerManager {

	private ConcurrentMap<Tpg, MessageQueuePartitionPuller> m_pullers = new ConcurrentHashMap<>();

	@Inject
	private MessageQueueManager m_queueManager;

	@Override
	public void startPuller(final Tpg tpg, TpgRelayer relayer) {
		if (!m_pullers.containsKey(tpg)) {
			m_pullers.putIfAbsent(tpg, new DefaultMessageQueuePartitionPuller(tpg, relayer, m_queueManager,
			      new ShutdownListener() {

				      @Override
				      public void onShutdown() {
					      m_pullers.remove(tpg);
				      }
			      }));
		}
		m_pullers.get(tpg).start();
	}

}
