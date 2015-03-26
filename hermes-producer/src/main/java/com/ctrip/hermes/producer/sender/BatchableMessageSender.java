package com.ctrip.hermes.producer.sender;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.ctrip.hermes.endpoint.EndpointChannel;
import com.ctrip.hermes.endpoint.EndpointChannelManager;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.ProducerMessage;
import com.ctrip.hermes.producer.api.SendResult;
import com.ctrip.hermes.producer.sender.AbstractMessageSender;
import com.ctrip.hermes.producer.sender.MessageSender;
import com.ctrip.hermes.remoting.command.SendMessageCommand;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BatchableMessageSender extends AbstractMessageSender implements MessageSender {

	private ConcurrentMap<Endpoint, EndpointWritingWorkerThread> m_workers = new ConcurrentHashMap<>();

	@Override
	public Future<SendResult> doSend(ProducerMessage<?> msg) {

		Endpoint endpoint = m_endpointManager.getEndpoint(msg.getTopic(), msg.getPartitionNo());

		createWorkerIfNeeded(endpoint);

		return m_workers.get(endpoint).submit(msg);
	}

	private void createWorkerIfNeeded(Endpoint endpoint) {
		if (!m_workers.containsKey(endpoint)) {
			synchronized (m_workers) {
				if (!m_workers.containsKey(endpoint)) {
					EndpointWritingWorkerThread worker = new EndpointWritingWorkerThread(endpoint, m_endpointChannelManager);

					worker.setDaemon(true);
					worker.setName("ProducerChannelWorkerThread-Channel-" + endpoint.getId());
					worker.start();

					m_workers.put(endpoint, worker);
				}
			}
		}
	}

	/**
	 * 
	 * @author Leo Liang(jhliang@ctrip.com)
	 *
	 */
	private static class EndpointWritingWorkerThread extends Thread {

		private BlockingQueue<ProducerChannelWorkerContext> m_queue = new LinkedBlockingQueue<>();

		private EndpointChannelManager m_endpointChannelManager;

		private Endpoint m_endpoint;

		private static final int BATCH_SIZE = 10;

		private static final int INTERVAL_MILLISECONDS = 50;

		public EndpointWritingWorkerThread(Endpoint endpoint, EndpointChannelManager endpointChannelManager) {
			m_endpointChannelManager = endpointChannelManager;
			m_endpoint = endpoint;
		}

		public Future<SendResult> submit(ProducerMessage<?> msg) {
			SettableFuture<SendResult> future = SettableFuture.create();

			m_queue.offer(new ProducerChannelWorkerContext(msg, future));

			return future;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			while (!Thread.interrupted()) {
				try {
					List<ProducerChannelWorkerContext> contexts = new ArrayList<>();
					m_queue.drainTo(contexts, BATCH_SIZE);

					if (!contexts.isEmpty()) {
						SendMessageCommand command = new SendMessageCommand();
						for (ProducerChannelWorkerContext context : contexts) {
							command.addMessage(context.m_msg, context.m_future);
						}

						EndpointChannel channel = m_endpointChannelManager.getChannel(m_endpoint);

						channel.write(command);

					}

					TimeUnit.MILLISECONDS.sleep(INTERVAL_MILLISECONDS);

				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (Exception e) {

				}
			}
		}

		private static class ProducerChannelWorkerContext {
			private ProducerMessage<?> m_msg;

			private SettableFuture<SendResult> m_future;

			/**
			 * @param msg
			 * @param future
			 */
			public ProducerChannelWorkerContext(ProducerMessage<?> msg, SettableFuture<SendResult> future) {
				m_msg = msg;
				m_future = future;
			}

		}

	}

}
