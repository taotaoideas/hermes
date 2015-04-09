package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.ConsumerMessageBatch;
import com.ctrip.hermes.core.transport.command.ConsumeMessageCommand;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultMessageQueuePuller implements MessageQueuePuller {

	private Tpg m_tpg;

	private Thread m_workerThread;

	private AtomicBoolean m_started = new AtomicBoolean(false);

	private List<Pair<Long, EndpointChannel>> m_endpoints = new CopyOnWriteArrayList<>();

	private MessageQueueCursor m_queueCursor;

	public DefaultMessageQueuePuller(Tpg tpg) {
		m_tpg = tpg;
		m_workerThread = new Thread(new PullerTask());
		m_workerThread.setDaemon(true);
		m_workerThread.setName(String.format("PullerThread-%s-%d-%s", m_tpg.getTopic(), m_tpg.getPartition(),
		      m_tpg.getGroupId()));
	}

	public void start() {
		if (m_started.compareAndSet(false, true)) {
			m_queueCursor = PlexusComponentLocator.lookup(MessageQueueManager.class).createCursor(m_tpg);
			m_workerThread.start();
		}
	}

	public void addEndpoint(long correlationId, EndpointChannel channel) {
		m_endpoints.add(new Pair<>(correlationId, channel));
	}

	protected ConsumerMessageBatch pullMessages(int batchSize) {
		return m_queueCursor.next(batchSize);
	}

	private class PullerTask implements Runnable {

		@Override
		public void run() {
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
						batch = pullMessages(100);
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
					endpointPos = m_endpoints.size() == 0 ? 0 : ++endpointPos % m_endpoints.size();
				}
			}

		}

	}
}
