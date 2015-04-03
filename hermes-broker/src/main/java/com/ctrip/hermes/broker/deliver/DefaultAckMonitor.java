package com.ctrip.hermes.broker.deliver;

import java.util.List;


public class DefaultAckMonitor<T> implements AckMonitor<T> {

	/* (non-Javadoc)
	 * @see com.ctrip.hermes.broker.deliver.AckMonitor#delivered(java.util.List, java.lang.Object)
	 */
   @Override
   public void delivered(List<com.ctrip.hermes.broker.deliver.AckMonitor.Locatable> locatables, T ctx) {
	   // TODO Auto-generated method stub
	   
   }

	/* (non-Javadoc)
	 * @see com.ctrip.hermes.broker.deliver.AckMonitor#acked(com.ctrip.hermes.broker.deliver.AckMonitor.Locatable, com.ctrip.hermes.broker.deliver.AckMonitor.Ack)
	 */
   @Override
   public void acked(com.ctrip.hermes.broker.deliver.AckMonitor.Locatable locatable,
         com.ctrip.hermes.broker.deliver.AckMonitor.Ack ack) {
	   // TODO Auto-generated method stub
	   
   }

	/* (non-Javadoc)
	 * @see com.ctrip.hermes.broker.deliver.AckMonitor#addListener(com.ctrip.hermes.broker.deliver.AckStatusListener)
	 */
   @Override
   public void addListener(AckStatusListener<T> listener) {
	   // TODO Auto-generated method stub
	   
   }
//
//	/**
//    * @author Leo Liang(jhliang@ctrip.com)
//    *
//    */
//   public interface ContinuousRange {
//
//   }
//
//	private List<AckStatusListener<T>> m_listeners = new ArrayList<>();
//
//	private List<Batch> m_batches = new LinkedList<>();
//
//	private int m_timeout;
//
//	private int m_timeoutCheckInterval;
//
//	private BlockingQueue<Triple<TaskType, Object, Object>> m_tasks = new LinkedBlockingQueue<>();
//
//	private enum TaskType {
//		Deliver, Ack;
//	}
//
//	public DefaultAckMonitor(int timeout, int timeoutCheckInterval) {
//		m_timeout = timeout;
//		m_timeoutCheckInterval = timeoutCheckInterval;
//
//		startTimeoutCheckTask();
//	}
//
//	private void startTimeoutCheckTask() {
//		Threads.forGroup("DefaultAckMonitorTaskExecutor").start(new Runnable() {
//
//			@Override
//			public void run() {
//				try {
//					doRun();
//				} catch (InterruptedException e) {
//				}
//			}
//
//			@SuppressWarnings("unchecked")
//			private void doRun() throws InterruptedException {
//				long lastTimeoutScan = -1;
//				while (true) {
//					// scan timeout when regularly
//					if (System.currentTimeMillis() - lastTimeoutScan > m_timeoutCheckInterval) {
//						lastTimeoutScan = System.currentTimeMillis();
//						scan();
//					}
//
//					Triple<TaskType, Object, Object> task = m_tasks.poll(m_timeoutCheckInterval, TimeUnit.MILLISECONDS);
//					if (task != null) {
//						switch (task.getFirst()) {
//						case Deliver:
//							doDelivered((List<Locatable>) task.getMiddle(), (T) task.getLast());
//							break;
//						case Ack:
//							doAcked((Locatable) task.getMiddle(), (Ack) task.getLast());
//							break;
//						}
//					}
//				}
//			}
//
//		});
//
//	}
//
//	private void scan() {
//		Iterator<Batch> it = m_batches.iterator();
//		while (it.hasNext()) {
//			Batch batch = it.next();
//			if (batch.isTimeout(m_timeout) || batch.allAcked()) {
//				batchDone(batch);
//				it.remove();
//			} else {
//				break;
//			}
//		}
//	}
//
//	private void batchDone(Batch batch) {
//		EWAHCompressedBitmap bm = batch.getBitmap();
//		int firstTimeoutOffset = bm.getFirstSetBit();
//		int timeoutStart = firstTimeoutOffset >= 0 ? firstTimeoutOffset : Integer.MAX_VALUE;
//		int timeoutEnd = firstTimeoutOffset >= 0 ? firstTimeoutOffset : Integer.MIN_VALUE;
//		if (firstTimeoutOffset >= 0) {
//			// TODO reverse iterator has bug?
//			IntIterator it = bm.intIterator();
//			while (it.hasNext()) {
//				timeoutEnd = it.next();
//			}
//		}
//
//		ContinuousRange failRange = batch.mergeTimeoutRange(timeoutStart, timeoutEnd);
//		if (failRange != null) {
//			notifyListeners(failRange, false, batch.getCtx());
//		}
//
//		notifyListeners(batch.getRange(), true, batch.getCtx());
//	}
//
//	@Override
//	public void delivered(List<Locatable> locatables, T ctx) {
//		m_tasks.offer(new Triple<TaskType, Object, Object>(TaskType.Deliver, locatables, ctx));
//	}
//
//	private void doDelivered(List<Locatable> locatables, T ctx) {
//		addBatch(toRange(locatables), newBitmap(locatables), ctx);
//	}
//
//	private Range toRange(List<Locatable> locatables) {
//		return new ContinuousRange(CollectionUtil.first(locatables).getOffset(), //
//		      CollectionUtil.last(locatables).getOffset());
//	}
//
//	private void addBatch(Range range, EWAHCompressedBitmap bm, T ctx) {
//		m_batches.add(new Batch(range, bm, ctx, System.currentTimeMillis()));
//	}
//
//	private EWAHCompressedBitmap newBitmap(Collection<? extends Locatable> locatables) {
//		EWAHCompressedBitmap bm = new EWAHCompressedBitmap();
//
//		for (Locatable l : locatables) {
//			// TODO translate long to int
//			bm.set((int) l.getOffset().getOffset());
//		}
//		return bm;
//	}
//
//	@Override
//	public void acked(Locatable locatable, Ack ack) {
//		m_tasks.offer(new Triple<TaskType, Object, Object>(TaskType.Ack, locatable, ack));
//	}
//
//	private void doAcked(Locatable locatable, Ack ack) {
//		Batch batch = findBatch(locatable);
//
//		if (batch == null) {
//			// TODO
//			System.out.println(String.format("batch for %s not found", locatable));
//		} else {
//			System.out.println("Ack " + ack + " " + locatable.getOffset().getOffset());
//			if (ack == Ack.FAIL) {
//				batch.updateFailRange(locatable.getOffset().getOffset());
//			}
//
//			EWAHCompressedBitmap bm = batch.getBitmap();
//			// TODO translate long to int
//			bm.clear((int) locatable.getOffset().getOffset());
//
//			if (batch.allAcked() && isFirstBatch(batch)) {
//				scan();
//			}
//		}
//	}
//
//	private boolean isFirstBatch(Batch batch) {
//		return m_batches.get(0) == batch;
//	}
//
//	private void notifyListeners(Range range, boolean success, T ctx) {
//		for (AckStatusListener<T> l : m_listeners) {
//			if (success) {
//				l.onSuccess(range, ctx);
//			} else {
//				l.onFail(range, ctx);
//			}
//		}
//	}
//
//	private Batch findBatch(Locatable locatable) {
//		for (Batch batch : m_batches) {
//			if (batch.contains(locatable)) {
//				return batch;
//			}
//		}
//		return null;
//	}
//
//	@Override
//	public void addListener(AckStatusListener<T> listener) {
//		m_listeners.add(listener);
//	}
//
//	protected boolean isTimeout(long start, int timeout) {
//		return System.currentTimeMillis() > start + timeout;
//	}
//
//	private class Batch {
//		private Range m_range;
//
//		private EWAHCompressedBitmap m_bitmap;
//
//		private T m_ctx;
//
//		private long m_ts;
//
//		private int m_failStart = Integer.MAX_VALUE;
//
//		private int m_failEnd = Integer.MIN_VALUE;
//
//		public Batch(Range range, EWAHCompressedBitmap bm, T ctx, long ts) {
//			m_range = range;
//			m_bitmap = bm;
//			m_ctx = ctx;
//			m_ts = ts;
//		}
//
//		public ContinuousRange mergeTimeoutRange(int timeoutStart, int timeoutEnd) {
//			int start = m_failStart;
//			int end = m_failEnd;
//
//			start = Math.min(start, timeoutStart);
//			end = Math.max(end, timeoutEnd);
//
//			if (start <= end) {
//				return new ContinuousRange(m_range.getId(), start, end);
//			} else {
//				return null;
//			}
//		}
//
//		public void updateFailRange(long failedOffset) {
//			if (failedOffset < m_failStart) {
//				m_failStart = (int) failedOffset;
//			}
//
//			if (failedOffset > m_failEnd) {
//				m_failEnd = (int) failedOffset;
//			}
//		}
//
//		public boolean allAcked() {
//			return m_bitmap.isEmpty();
//		}
//
//		public boolean isTimeout(int timeout) {
//			return DefaultAckMonitor.this.isTimeout(m_ts, timeout);
//		}
//
//		public boolean contains(Locatable locatable) {
//			return m_range.contains(locatable.getOffset());
//		}
//
//		public EWAHCompressedBitmap getBitmap() {
//			return m_bitmap;
//		}
//
//		public T getCtx() {
//			return m_ctx;
//		}
//
//		public Range getRange() {
//			return m_range;
//		}
//
//	}

}
