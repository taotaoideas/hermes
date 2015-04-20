package com.ctrip.hermes.broker.transport.transmitter;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * <pre>
 * Mapping to one physical channel belongs to one {@linkplain Tpg Tpg} .
 * TpgChannel can use as the key of Map/Set. 
 * Two TpgChannels are considered as same, while they have the same {@linkplain EndpointChannel physcial channel} and {@linkplain Tpg Tpg} and correlationId.
 * </pre>
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class TpgChannel {

	private static final int DEFAULT_QUEUE_CAPACITY = 100;

	private long m_correlationId;

	private Tpg m_tpg;

	private EndpointChannel m_channel;

	private LinkedList<TppConsumerMessageBatch> m_queue;

	private AtomicInteger m_window = new AtomicInteger(0);

	private int m_pendingSize = 0;

	private ReentrantReadWriteLock m_rwLock = new ReentrantReadWriteLock();

	private AtomicBoolean m_closed = new AtomicBoolean(false);

	public TpgChannel(Tpg tpg, long correlationId, EndpointChannel channel, int window) {
		m_tpg = tpg;
		m_correlationId = correlationId;
		m_channel = channel;
		m_window.set(window);
		m_queue = new LinkedList<>();
	}

	public Tpg getTpg() {
		return m_tpg;
	}

	public boolean isClosed() {
		return m_closed.get();
	}

	public void close() {
		m_closed.set(true);
	}

	public int availableSize() {
		m_rwLock.readLock().lock();
		try {
			// TODO DEFAULT_QUEUE_CAPACITY
			if (m_queue.size() < DEFAULT_QUEUE_CAPACITY) {
				return m_window.get() - m_pendingSize;
			} else {
				return 0;
			}
		} finally {
			m_rwLock.readLock().unlock();
		}
	}

	public void transmit(List<TppConsumerMessageBatch> batches) {
		m_rwLock.writeLock().lock();
		try {
			for (TppConsumerMessageBatch batch : batches) {
				m_queue.addLast(batch);
				m_pendingSize += batch.size();
			}
		} finally {
			m_rwLock.writeLock().unlock();
		}
	}

	public void setWindow(int window) {
		m_window.set(window);
	}

	public long getCorrelationId() {
		return m_correlationId;
	}

	public TpgChannelFetchResult fetch(int batchSize) {
		m_rwLock.writeLock().lock();
		try {
			List<TppConsumerMessageBatch> batches = new ArrayList<>();
			int remainingSize = batchSize;
			while (remainingSize > 0) {
				if (m_queue.isEmpty()) {
					break;
				}

				if (m_queue.peek().size() <= remainingSize) {
					TppConsumerMessageBatch tmp = m_queue.poll();
					batches.add(tmp);
					m_pendingSize -= tmp.size();
					remainingSize -= tmp.size();
				} else {
					break;
				}
			}

			if (!batches.isEmpty()) {
				return new TpgChannelFetchResult(batchSize - remainingSize, batches);
			} else {
				return null;
			}
		} finally {
			m_rwLock.writeLock().unlock();
		}

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_channel == null) ? 0 : m_channel.hashCode());
		result = prime * result + (int) (m_correlationId ^ (m_correlationId >>> 32));
		result = prime * result + ((m_tpg == null) ? 0 : m_tpg.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TpgChannel other = (TpgChannel) obj;
		if (m_channel == null) {
			if (other.m_channel != null)
				return false;
		} else if (!m_channel.equals(other.m_channel))
			return false;
		if (m_correlationId != other.m_correlationId)
			return false;
		if (m_tpg == null) {
			if (other.m_tpg != null)
				return false;
		} else if (!m_tpg.equals(other.m_tpg))
			return false;
		return true;
	}

	public static class TpgChannelFetchResult {
		private int m_size;

		private List<TppConsumerMessageBatch> m_batches;

		public TpgChannelFetchResult(int size, List<TppConsumerMessageBatch> batches) {
			m_size = size;
			m_batches = batches;
		}

		public int getSize() {
			return m_size;
		}

		public List<TppConsumerMessageBatch> getBatches() {
			return m_batches;
		}

	}

}
