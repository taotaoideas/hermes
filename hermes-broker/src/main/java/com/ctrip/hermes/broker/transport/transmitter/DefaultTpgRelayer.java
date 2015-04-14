package com.ctrip.hermes.broker.transport.transmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ctrip.hermes.core.message.TppConsumerMessageBatch;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultTpgRelayer implements TpgRelayer {

	private int m_pos = 0;

	private TpgChannel m_currentChannel;

	private AtomicBoolean m_closed = new AtomicBoolean(false);

	private List<TpgChannel> m_channels = new ArrayList<>();

	private ReentrantReadWriteLock m_rwLock = new ReentrantReadWriteLock();

	@Override
	public void close() {
		m_closed.set(true);
	}

	@Override
	public boolean isClosed() {
		return m_closed.get();
	}

	@Override
	public int availableSize() {
		m_rwLock.writeLock().lock();
		try {
			m_currentChannel = m_channels.get(m_pos);
			m_pos = (m_pos + 1) % m_channels.size();
			return m_currentChannel.availableSize();
		} finally {
			m_rwLock.writeLock().unlock();
		}
	}

	@Override
	public boolean relay(List<TppConsumerMessageBatch> batchs) {
		if (m_currentChannel != null && !m_currentChannel.isClosed()) {
			m_currentChannel.transmit(batchs);
			return true;
		}
		return false;
	}

	@Override
	public void addChannel(TpgChannel channel) {
		m_rwLock.writeLock().lock();
		try {
			m_channels.add(channel);
		} finally {
			m_rwLock.writeLock().unlock();
		}
	}

	@Override
	public boolean containsChannel(TpgChannel channel) {
		m_rwLock.readLock().lock();
		try {
			return m_channels.contains(channel);
		} finally {
			m_rwLock.readLock().unlock();
		}
	}

}
