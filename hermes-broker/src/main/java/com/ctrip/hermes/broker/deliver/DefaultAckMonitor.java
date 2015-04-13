package com.ctrip.hermes.broker.deliver;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.ctrip.hermes.core.utils.CollectionUtil;

public class DefaultAckMonitor<T> implements AckMonitor<T> {

	private List<Batch> m_batches = new LinkedList<>();

	private int m_timeout;

	public DefaultAckMonitor(int timeout) {
		m_timeout = timeout;
	}

	@Override
	public BatchResult scan() {
		BatchResult result = null;

		Iterator<Batch> it = m_batches.iterator();
		while (it.hasNext()) {
			Batch batch = it.next();
			if (batch.isTimeout(m_timeout) || batch.allAcked()) {
				it.remove();
				if (result == null) {
					result = batch.getResult();
				} else {
					result.merge(batch.getResult());
				}
			} else {
				break;
			}
		}

		return result;
	}

	@Override
	public void delivered(EnumRange<T> range) {
		m_batches.add(new Batch(range, System.currentTimeMillis()));
	}

	@Override
	public void acked(Locatable<T> locatable, boolean success) {
		Batch batch = findBatch(locatable);

		if (batch == null) {
			// TODO
			System.out.println(String.format("batch for %s not found", locatable));
		} else {
			batch.updateState(locatable.getOffset(), success);
		}
	}

	private Batch findBatch(Locatable<T> locatable) {
		for (Batch batch : m_batches) {
			if (batch.contains(locatable)) {
				return batch;
			}
		}
		return null;
	}

	protected boolean isTimeout(long start, int timeout) {
		return System.currentTimeMillis() > start + timeout;
	}

	private enum State {
		INIT, SUCCESS, FAIL
	}

	private class Batch {

		private ContinuousRange<?> m_continuousRange;

		private TreeMap<Long, State> m_map;

		private long m_ts;

		private int m_doneCount = 0;

		public Batch(EnumRange<T> range, long ts) {
			List<Long> rangeOffsets = range.getOffsets();
			m_continuousRange = new ContinuousRange<>(range.getId(), CollectionUtil.first(rangeOffsets),
			      CollectionUtil.last(rangeOffsets));

			m_ts = ts;

			m_map = new TreeMap<>();
			for (long offset : rangeOffsets) {
				m_map.put(offset, State.INIT);
			}
		}

		public BatchResult getResult() {
			return new BatchResult(getFailRange(), getDoneRange());
		}

		public ContinuousRange<?> getDoneRange() {
			return m_continuousRange;
		}

		public EnumRange<?> getFailRange() {
			EnumRange<?> failRange = new EnumRange<>(m_continuousRange.getId());

			for (Map.Entry<Long, State> entry : m_map.entrySet()) {
				if (entry.getValue() != State.SUCCESS) {
					failRange.addOffset(entry.getKey());
				}
			}

			if (failRange.getOffsets().isEmpty()) {
				return null;
			} else {
				return failRange;
			}
		}

		public void updateState(long offset, boolean success) {
			State oldState = m_map.put(offset, success ? State.SUCCESS : State.FAIL);
			if (oldState == State.INIT) {
				m_doneCount++;
			}
		}

		public boolean allAcked() {
			return m_doneCount == m_map.size();
		}

		public boolean isTimeout(int timeout) {
			return DefaultAckMonitor.this.isTimeout(m_ts, timeout);
		}

		public boolean contains(Locatable<T> locatable) {
			return m_continuousRange.getId().equals(locatable.getId()) && m_map.containsKey(locatable.getOffset());
		}

	}

}
