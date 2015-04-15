package com.ctrip.hermes.broker.ack;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.ctrip.hermes.core.utils.CollectionUtil;

public class DefaultAckHolder implements AckHolder {

	private List<Batch> m_batches = new LinkedList<>();

	private int m_timeout;

	public DefaultAckHolder(int timeout) {
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
	public void delivered(EnumRange range) {
		m_batches.add(new Batch(range, System.currentTimeMillis()));
	}

	@Override
	public void acked(long offset, boolean success) {
		Batch batch = findBatch(offset);

		if (batch == null) {
			// TODO
			System.out.println(String.format("batch for %s not found", offset));
		} else {
			batch.updateState(offset, success);
		}
	}

	private Batch findBatch(long offset) {
		for (Batch batch : m_batches) {
			if (batch.contains(offset)) {
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

		private ContinuousRange m_continuousRange;

		private TreeMap<Long, State> m_map;

		private long m_ts;

		private int m_doneCount = 0;

		public Batch(EnumRange range, long ts) {
			List<Long> rangeOffsets = range.getOffsets();
			m_continuousRange = new ContinuousRange(CollectionUtil.first(rangeOffsets), CollectionUtil.last(rangeOffsets));

			m_ts = ts;

			m_map = new TreeMap<>();
			for (long offset : rangeOffsets) {
				m_map.put(offset, State.INIT);
			}
		}

		public BatchResult getResult() {
			return new BatchResult(getFailRange(), getDoneRange());
		}

		public ContinuousRange getDoneRange() {
			return m_continuousRange;
		}

		public EnumRange getFailRange() {
			EnumRange failRange = new EnumRange();

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
			return DefaultAckHolder.this.isTimeout(m_ts, timeout);
		}

		public boolean contains(long offset) {
			return m_map.containsKey(offset);
		}

	}

}
