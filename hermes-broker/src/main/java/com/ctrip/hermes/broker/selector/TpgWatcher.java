package com.ctrip.hermes.broker.selector;

import java.util.List;

import com.ctrip.hermes.core.bo.Tpg;

public class TpgWatcher {

	private Tpg m_tpg;

	private List<Long> m_readOffsets;

	private Runnable m_runnable;

	public TpgWatcher(Tpg tpg, List<Long> readOffsets, Runnable runnable) {
		m_tpg = tpg;
		m_readOffsets = readOffsets;
		m_runnable = runnable;
	}

	public Tpg getTpg() {
		return m_tpg;
	}

	public List<Long> getReadOffsets() {
		return m_readOffsets;
	}

	public Runnable getRunnable() {
		return m_runnable;
	}

	public boolean isReadable(long writeOffset, int priority) {
		return m_readOffsets.get(priority) < writeOffset;
	}

}
