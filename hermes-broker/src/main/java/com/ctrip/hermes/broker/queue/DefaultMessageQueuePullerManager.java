package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.ConsumerMessageBatch;
import com.ctrip.hermes.core.transport.command.ConsumeMessageCommand;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultMessageQueuePullerManager implements MessageQueuePullerManager {

	@Inject
	private MessageQueueManager m_queueManager;

	@Inject
	private ConcurrentMap<Tpg, Puller> m_pullers = new ConcurrentHashMap<>();

	@Override
	public void startPuller(Tpg tpg, long correlationId, EndpointChannel channel) {
		m_pullers.putIfAbsent(tpg, new Puller(tpg));
		m_pullers.get(tpg).addEndpoint(correlationId, channel);
		m_pullers.get(tpg).start();
	}

	private class Puller {

		private Tpg m_tpg;

		private Thread m_workerThread;

		private AtomicBoolean m_started = new AtomicBoolean(false);

		private List<Pair<Long, EndpointChannel>> m_endpoints = new CopyOnWriteArrayList<>();

		private MessageQueueCursor m_queueCursor;

		public Puller(Tpg tpg) {
			m_tpg = tpg;
			m_workerThread = new Thread(new Runnable() {

				@Override
				public void run() {
					pull();
				}
			});
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

		public void addEndpoint(long correlationId, EndpointChannel channel) {
			m_endpoints.add(new Pair<>(correlationId, channel));
		}

		private void pull() {
			int endpointPos = 0;

			ConsumerMessageBatch batch = null;
			while (!Thread.currentThread().isInterrupted()) {
				try {
					if (m_endpoints.isEmpty()) {
						// TODO recycle this thread
						TimeUnit.SECONDS.sleep(1);
						continue;
					}

					if (batch == null) {
						// TODO recycle this thread
						// TODO get batch size
						batch = m_queueCursor.next(100);
					}

					boolean sent = false;
					if (batch != null) {
						Pair<Long, EndpointChannel> selectedEndpoint = m_endpoints.get(endpointPos);

						ConsumeMessageCommand cmd = new ConsumeMessageCommand();
						cmd.addMessage(selectedEndpoint.getKey(), batch);
						selectedEndpoint.getValue().writeCommand(cmd);
						batch = null;
						sent = true;
					}

					if (!sent) {
						// TODO consumer处理太慢，要考虑
						TimeUnit.MILLISECONDS.sleep(5);
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					// TODO
					e.printStackTrace();
				} finally {
					endpointPos = m_endpoints.size() == 0 ? 0 : endpointPos++ % m_endpoints.size();
				}
			}

		}
	}
}
