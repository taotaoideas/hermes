package com.ctrip.hermes.broker.queue.partition;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.broker.transport.transmitter.TpgRelayer;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultMessageQueuePartitionPuller implements MessageQueuePartitionPuller {

	private Tpg m_tpg;

	private ShutdownListener m_listener;

	private Thread m_workerThread;

	private AtomicBoolean m_started = new AtomicBoolean(false);

	private TpgRelayer m_relayer;

	private MessageQueuePartitionCursor m_queueCursor;

	private MessageQueueManager m_queueManager;

	public DefaultMessageQueuePartitionPuller(Tpg tpg, TpgRelayer relayer, MessageQueueManager queueManager,
	      ShutdownListener listener) {
		m_tpg = tpg;
		m_relayer = relayer;
		m_listener = listener;
		m_queueManager = queueManager;

		m_workerThread = new Thread(new PullerTask());
		m_workerThread.setDaemon(true);
		m_workerThread.setName(String.format("PullerThread-%s-%d-%s", m_tpg.getTopic(), m_tpg.getPartition(),
		      m_tpg.getGroupId()));
	}

	public void start() {
		if (m_started.compareAndSet(false, true)) {
			m_queueCursor = m_queueManager.createCursor(m_tpg);
			m_workerThread.start();
		}
	}

	protected List<TppConsumerMessageBatch> pullMessages(int batchSize) {
		return m_queueCursor.next(batchSize);
	}

	private class PullerTask implements Runnable {

		@Override
		public void run() {
			List<TppConsumerMessageBatch> batch = new LinkedList<>();

			while (!Thread.currentThread().isInterrupted()) {
				try {
					if (m_relayer == null) {
						TimeUnit.SECONDS.sleep(1);
						continue;
					}

					if (m_relayer.isClosed()) {
						// TODO log
						return;
					}

					int availableSize = m_relayer.availableSize();
					if (availableSize > 0) {
						if (batch.isEmpty()) {
							batch = pullMessages(availableSize);
						}

						if (m_relayer.relay(batch)) {
							batch.clear();
						}
					} else {
						TimeUnit.MILLISECONDS.sleep(50);
					}

				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					// TODO
					e.printStackTrace();
				} finally {
					m_listener.onShutdown();
				}
			}

		}
	}

}
