package com.ctrip.hermes.broker.ack;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = AckManager.class)
public class DefaultAckManager implements AckManager, Initializable {

	// TODO while consumer disconnect, clear holder and offset
	private ConcurrentMap<Triple<Tpp, String, Boolean>, AckHolder> m_holders = new ConcurrentHashMap<>();

	private Thread m_workerThread;

	@Inject
	private MessageQueueManager m_queueManager;

	@Inject
	private MetaService m_metaService;

	@Override
	public void initialize() throws InitializationException {
		m_workerThread = new Thread(new Runnable() {

			@Override
			public void run() {
				while (!Thread.currentThread().isInterrupted()) {
					try {
						for (Map.Entry<Triple<Tpp, String, Boolean>, AckHolder> entry : m_holders.entrySet()) {
							AckHolder holder = entry.getValue();
							Tpp tpp = entry.getKey().getFirst();
							String groupId = entry.getKey().getMiddle();
							boolean resend = entry.getKey().getLast();

							BatchResult result = holder.scan();

							if (result != null) {
								ContinuousRange doneRange = result.getDoneRange();
								EnumRange failRange = result.getFailRange();
								if (failRange != null) {
									m_queueManager.nack(tpp, groupId, resend, failRange.getOffsets());
								}

								if (doneRange != null) {
									m_queueManager.ack(tpp, groupId, resend, doneRange.getEnd());
								}
							}
						}

					} catch (Exception e) {
						// TODO
						e.printStackTrace();
					} finally {
						// TODO
						try {
							TimeUnit.MILLISECONDS.sleep(50);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
				}
			}
		});
		// TODO
		m_workerThread.setDaemon(true);
		m_workerThread.setName("AckManagerWorker");
		m_workerThread.start();
	}

	@Override
	public void delivered(Tpp tpp, String groupId, boolean resend, Collection<Long> msgSeqs) {
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		ensureMapEntryExist(key);
		EnumRange range = new EnumRange(new ArrayList<>(msgSeqs));
		m_holders.get(key).delivered(range);
	}

	private void ensureMapEntryExist(Triple<Tpp, String, Boolean> key) {
		if (!m_holders.containsKey(key)) {
			m_holders.putIfAbsent(key, new DefaultAckHolder(
			      m_metaService.getAckTimeoutSeconds(key.getFirst().getTopic()) * 1000));
		}
	}

	@Override
	public void acked(Tpp tpp, String groupId, boolean resend, Collection<Long> msgSeqs) {
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		ensureMapEntryExist(key);
		for (Long msgSeq : msgSeqs) {
			m_holders.get(key).acked(msgSeq, true);
		}
	}

	@Override
	public void nacked(Tpp tpp, String groupId, boolean resend, Collection<Long> msgSeqs) {
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		ensureMapEntryExist(key);
		for (Long msgSeq : msgSeqs) {
			m_holders.get(key).acked(msgSeq, false);
		}
	}

}
