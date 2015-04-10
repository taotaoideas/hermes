package com.ctrip.hermes.broker.transport.transmitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.ConsumerMessageBatch;
import com.ctrip.hermes.core.transport.command.ConsumeMessageCommand;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MessageTransmitter.class)
public class DefaultMessageTransmitter implements MessageTransmitter {
	// one physical channel mapping to one woker
	// one tpg mapping to one relay
	// one physical channel mapping to multiple tpg, but each <physcal channel, tpg> only mapping to one <tpgchannel, correlationId>
	private Map<EndpointChannel, TransmitterWorker> m_channel2Worker = new HashMap<>();

	private Map<Tpg, TpgRelay> m_tpg2Relay = new HashMap<>();

	@Override
	public synchronized TpgRelay registerDestination(Tpg tpg, long correlationId, EndpointChannel channel, int window) {
		if (!m_channel2Worker.containsKey(channel)) {
			TransmitterWorker worker = new TransmitterWorker(channel);
			worker.start();
			// TODO channel shutdown hook

			m_channel2Worker.put(channel, worker);
		}

		if (!m_tpg2Relay.containsKey(tpg)) {
			m_tpg2Relay.put(tpg, new DefaultTpgRelay());
		}

		TpgRelay relay = m_tpg2Relay.get(tpg);
		TransmitterWorker worker = m_channel2Worker.get(channel);
		TpgChannel tpgChannel = new TpgChannel(tpg, correlationId, channel, window);

		if (!relay.containsChannel(tpgChannel)) {
			relay.addChannel(tpgChannel);
		}

		if (!worker.containsTpgChannel(tpgChannel)) {
			worker.addTpgChannel(tpgChannel);
		}

		return m_tpg2Relay.get(tpg);

	}

	private static class TransmitterWorker {
		private List<TpgChannel> m_tpgChannels = new ArrayList<>();

		private Thread m_workerThread;

		private EndpointChannel m_channel;

		private ReentrantReadWriteLock m_rwLock = new ReentrantReadWriteLock();

		public TransmitterWorker(EndpointChannel channel) {
			m_channel = channel;
		}

		public void addTpgChannel(TpgChannel tpgChannel) {
			m_rwLock.writeLock().lock();
			try {
				m_tpgChannels.add(tpgChannel);
			} finally {
				m_rwLock.writeLock().unlock();
			}
		}

		public boolean containsTpgChannel(TpgChannel tpgChannel) {
			m_rwLock.readLock().lock();
			try {
				return m_tpgChannels.contains(tpgChannel);
			} finally {
				m_rwLock.readLock().unlock();
			}
		}

		public void start() {
			m_workerThread = new Thread(new Runnable() {

				@Override
				public void run() {
					int startPos = 0;

					while (!Thread.currentThread().isInterrupted()) {
						try {
							if (m_tpgChannels.isEmpty()) {
								TimeUnit.SECONDS.sleep(1);
								continue;
							}

							// TODO traffic control(batchSize must larger than windowSize of tpgChannel)
							int batchSize = 100;
							ConsumeMessageCommand cmd = new ConsumeMessageCommand();

							m_rwLock.readLock().lock();
							try {
								// TODO start with different pos each time
								for (int i = 0; i < m_tpgChannels.size(); i++) {
									if (batchSize <= 0) {
										break;
									}

									TpgChannel tpgChannel = m_tpgChannels.get((startPos + i) % m_tpgChannels.size());

									ConsumerMessageBatch batch = tpgChannel.fetch(batchSize);

									if (batch != null && batch.size() > 0) {
										cmd.addMessage(tpgChannel.getCorrelationId(), batch);
										batchSize -= batch.size();
									}
								}
								startPos = (startPos + 1) % m_tpgChannels.size();

							} finally {
								m_rwLock.readLock().unlock();
							}

							if (!cmd.getMsgs().isEmpty()) {
								m_channel.writeCommand(cmd);
							}

							TimeUnit.MILLISECONDS.sleep(10);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						} catch (Exception e) {
							// TODO
							e.printStackTrace();
						}
					}
				}

			});
			m_workerThread.setDaemon(true);
			m_workerThread.setName(String.format("TransmitterWorker-%s", m_channel));
			m_workerThread.start();
		}

	}
}
