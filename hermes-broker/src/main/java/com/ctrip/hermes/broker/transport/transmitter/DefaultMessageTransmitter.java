package com.ctrip.hermes.broker.transport.transmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.helper.Threads;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.ack.AckManager;
import com.ctrip.hermes.broker.transport.transmitter.TpgChannel.TpgChannelFetchResult;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.transport.command.ConsumeMessageCommand;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MessageTransmitter.class)
public class DefaultMessageTransmitter implements MessageTransmitter, Initializable {
	// one physical channel mapping to one woker
	// one tpg mapping to one relayer
	// one physical channel mapping to multiple tpg, but each <physcal channel, tpg> only mapping to one <tpgchannel, correlationId>
	private Map<EndpointChannel, TransmitterWorker> m_channel2Worker = new ConcurrentHashMap<>();

	private Map<Tpg, TpgRelayer> m_tpg2Relayer = new ConcurrentHashMap<>();

	@Inject
	private AckManager m_ackManager;

	@Override
	public synchronized TpgRelayer registerDestination(Tpg tpg, long correlationId, EndpointChannel channel, int window) {
		if (!m_channel2Worker.containsKey(channel)) {
			TransmitterWorker worker = new TransmitterWorker(channel);
			worker.start();
			// TODO channel shutdown hook

			m_channel2Worker.put(channel, worker);
		}

		if (!m_tpg2Relayer.containsKey(tpg)) {
			m_tpg2Relayer.put(tpg, new DefaultTpgRelayer());
		}

		TpgRelayer relayer = m_tpg2Relayer.get(tpg);
		TransmitterWorker worker = m_channel2Worker.get(channel);
		TpgChannel tpgChannel = new TpgChannel(tpg, correlationId, channel, window);

		if (!relayer.containsChannel(tpgChannel)) {
			relayer.addChannel(tpgChannel);
		}

		if (!worker.containsTpgChannel(tpgChannel)) {
			worker.addTpgChannel(tpgChannel);
		}

		return m_tpg2Relayer.get(tpg);

	}

	private class TransmitterWorker {
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
						if (m_channel.isClosed()) {
							m_channel2Worker.remove(m_channel);
							break;
						}

						try {
							if (m_tpgChannels.isEmpty()) {
								TimeUnit.SECONDS.sleep(1);
								continue;
							}

							// TODO traffic control(batchSize must larger than windowSize of tpgChannel)
							int batchSize = Integer.MAX_VALUE;
							ConsumeMessageCommand cmd = new ConsumeMessageCommand();

							m_rwLock.readLock().lock();
							try {
								// TODO start with different pos each time
								for (int i = 0; i < m_tpgChannels.size(); i++) {
									if (batchSize <= 0) {
										break;
									}

									TpgChannel tpgChannel = m_tpgChannels.get((startPos + i) % m_tpgChannels.size());

									TpgChannelFetchResult result = tpgChannel.fetch(batchSize);

									if (result != null && result.getBatches() != null && !result.getBatches().isEmpty()) {
										cmd.addMessage(tpgChannel.getCorrelationId(), result.getBatches());
										batchSize -= result.getSize();

										// notify ack manager
										for (TppConsumerMessageBatch batch : result.getBatches()) {
											m_ackManager.delivered(
											      new Tpp(batch.getTopic(), batch.getPartition(), batch.isPriority()), tpgChannel
											            .getTpg().getGroupId(), batch.isResend(), batch.getMsgSeqs());
										}
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

	private class HouseKeepingTask implements Runnable {

		@Override
		public void run() {
			while (!Thread.currentThread().isInterrupted()) {
				for (Map.Entry<Tpg, TpgRelayer> entry : m_tpg2Relayer.entrySet()) {
					closeIdleTpgRelayer(entry.getKey(), entry.getValue());
				}

				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	private synchronized void closeIdleTpgRelayer(Tpg tpg, TpgRelayer tpgRelayer) {
		if (tpgRelayer.channleCount() == 0) {
			tpgRelayer.close();
			m_tpg2Relayer.remove(tpg);
		}
	}

	@Override
	public void initialize() throws InitializationException {
		Threads.forGroup("TransmitterHouseKeeping").start(new HouseKeepingTask());
	}
}
