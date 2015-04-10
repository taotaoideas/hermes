package com.ctrip.hermes.broker.deliver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.unidal.helper.Threads;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.core.utils.CollectionUtil;

public class DefaultAckMonitor<T> implements AckMonitor<T> {

	private List<AckStatusListener<T>> m_listeners = new ArrayList<>();

	private List<Batch> m_batches = new LinkedList<>();

	private int m_timeout;

	private int m_timeoutCheckInterval;

	private BlockingQueue<Triple<TaskType, Object, Object>> m_tasks = new LinkedBlockingQueue<>();

	private enum TaskType {
		Deliver, Ack;
	}

	public DefaultAckMonitor(int timeout, int timeoutCheckInterval) {
		m_timeout = timeout;
		m_timeoutCheckInterval = timeoutCheckInterval;

		startTimeoutCheckTask();
	}

	private void startTimeoutCheckTask() {
		Threads.forGroup("DefaultAckMonitorTaskExecutor").start(new Runnable() {

			@Override
			public void run() {
				try {
					doRun();
				} catch (InterruptedException e) {
				}
			}

			@SuppressWarnings("unchecked")
			private void doRun() throws InterruptedException {
				long lastTimeoutScan = -1;
				while (true) {
					// scan timeout when regularly
					if (System.currentTimeMillis() - lastTimeoutScan > m_timeoutCheckInterval) {
						lastTimeoutScan = System.currentTimeMillis();
						scan();
					}

					Triple<TaskType, Object, Object> task = m_tasks.poll(m_timeoutCheckInterval, TimeUnit.MILLISECONDS);
					if (task != null) {
						switch (task.getFirst()) {
						case Deliver:
							doDelivered((EnumRange<?>) task.getMiddle(), (T) task.getLast());
							break;
						case Ack:
							doAcked((Locatable) task.getMiddle(), (Boolean) task.getLast());
							break;
						}
					}
				}
			}

		});

	}

	private void scan() {
		Iterator<Batch> it = m_batches.iterator();
		while (it.hasNext()) {
			Batch batch = it.next();
			if (batch.isTimeout(m_timeout) || batch.allAcked()) {
				batchDone(batch);
				it.remove();
			} else {
				break;
			}
		}
	}

	private void batchDone(Batch batch) {
		EnumRange<?> failRange = batch.getFailRange();
		if (failRange != null) {
			notifyFail(failRange, batch.getCtx());
		}

		notifySuccess(batch.getSuccessRange(), batch.getCtx());
	}

	private void notifySuccess(ContinuousRange<?> successRange, T ctx) {
		for (AckStatusListener<T> l : m_listeners) {
			l.onSuccess(successRange, ctx);
		}

	}

	private void notifyFail(EnumRange<?> failRange, T ctx) {
		for (AckStatusListener<T> l : m_listeners) {
			l.onFail(failRange, ctx);
		}
	}

	@Override
	public void delivered(EnumRange<?> range, T ctx) {
		m_tasks.offer(new Triple<TaskType, Object, Object>(TaskType.Deliver, range, ctx));
	}

	private void doDelivered(EnumRange<?> range, T ctx) {
		m_batches.add(new Batch(range, ctx, System.currentTimeMillis()));
	}

	@Override
	public void acked(Locatable locatable, boolean success) {
		m_tasks.offer(new Triple<TaskType, Object, Object>(TaskType.Ack, locatable, success));
	}

	private void doAcked(Locatable locatable, boolean success) {
		Batch batch = findBatch(locatable);

		if (batch == null) {
			// TODO
			System.out.println(String.format("batch for %s not found", locatable));
		} else {
			batch.updateState(locatable.getOffset(), success);

			if (batch.allAcked() && isFirstBatch(batch)) {
				scan();
			}
		}
	}

	private boolean isFirstBatch(Batch batch) {
		return m_batches.get(0) == batch;
	}

	private Batch findBatch(Locatable locatable) {
		for (Batch batch : m_batches) {
			if (batch.contains(locatable)) {
				return batch;
			}
		}
		return null;
	}

	@Override
	public void addListener(AckStatusListener<T> listener) {
		m_listeners.add(listener);
	}

	protected boolean isTimeout(long start, int timeout) {
		return System.currentTimeMillis() > start + timeout;
	}

	private enum State {
		INIT, SUCCESS, FAIL
	}

	private class Batch {

		private ContinuousRange<?> m_continuousRange;

		private TreeMap<Long, State> m_bitmap;

		private T m_ctx;

		private long m_ts;

		private int m_doneCount = 0;

		public Batch(EnumRange<?> range, T ctx, long ts) {
			List<Long> rangeOffsets = range.getOffsets();
			m_continuousRange = new ContinuousRange<>(range.getId(), CollectionUtil.first(rangeOffsets),
			      CollectionUtil.last(rangeOffsets));

			m_ctx = ctx;
			m_ts = ts;

			m_bitmap = new TreeMap<>();
			for (long offset : rangeOffsets) {
				m_bitmap.put(offset, State.INIT);
			}
		}

		public ContinuousRange<?> getSuccessRange() {
			return m_continuousRange;
		}

		public EnumRange<?> getFailRange() {
			EnumRange<?> failRange = new EnumRange<>(m_continuousRange.getId());

			for (Map.Entry<Long, State> entry : m_bitmap.entrySet()) {
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
			State oldState = m_bitmap.put(offset, success ? State.SUCCESS : State.FAIL);
			if (oldState == State.INIT) {
				m_doneCount++;
			}
		}

		public boolean allAcked() {
			return m_doneCount == m_bitmap.size();
		}

		public boolean isTimeout(int timeout) {
			return DefaultAckMonitor.this.isTimeout(m_ts, timeout);
		}

		public boolean contains(Locatable locatable) {
			return m_continuousRange.getId().equals(locatable.getId()) && m_bitmap.containsKey(locatable.getOffset());
		}

		public T getCtx() {
			return m_ctx;
		}

	}

}
