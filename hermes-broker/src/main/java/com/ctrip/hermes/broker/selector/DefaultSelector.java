package com.ctrip.hermes.broker.selector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;

public class DefaultSelector implements Selector {

	private BlockingQueue<Runnable> m_tasks = new LinkedBlockingQueue<Runnable>();

	private Map<Tpp, Long> m_writeOffsets = new HashMap<>();

	private List<TpgWatcher> m_watchers = new LinkedList<>();

	@Override
	public void registerReadOp(final Tpg tpg, final Runnable runnable) {
		m_tasks.offer(new Runnable() {

			@Override
			public void run() {
				initWriteOffsets(tpg);
				m_watchers.add(new TpgWatcher(tpg, queryReadOffset(tpg.getTopic(), tpg.getPartition()), runnable));
			}

		});
	}

	private void initWriteOffsets(final Tpg tpg) {
		// TODO
		for (int priority = 0; priority < 2; priority++) {
			Tpp tpp = new Tpp(tpg.getTopic(), tpg.getPartition(), priority == 0 ? true : false);
			if (m_writeOffsets.get(tpp) == null) {
				addInitWriteOffsetTask(tpp);
			}
		}
	}

	private void addInitWriteOffsetTask(final Tpp tpp) {
		m_tasks.offer(new Runnable() {

			@Override
			public void run() {
				if (!m_writeOffsets.containsKey(tpp)) {
					m_writeOffsets.put(tpp, queryWriteOffset(tpp));
				}
			}

		});
	}

	private List<Long> queryReadOffset(String topic, int partition) {
		// TODO
		return Arrays.asList(0L, 0L);
	}

	private long queryWriteOffset(Tpp tpp) {
		// TODO
		return 0;
	}

	@Override
	public void updateWriteOffset(final Tpp tpp, final long newWriteOffset) {
		m_tasks.offer(new Runnable() {

			@Override
			public void run() {
				Long oldWriteOffset = m_writeOffsets.get(tpp);
				if (oldWriteOffset == null || newWriteOffset > oldWriteOffset) {
					m_writeOffsets.put(tpp, newWriteOffset);
					scan();
				}
			}

		});
	}

	private void scan() {
		Iterator<TpgWatcher> iter = m_watchers.iterator();

		while (iter.hasNext()) {
			TpgWatcher watcher = iter.next();
			Tpg tpg = watcher.getTpg();
			boolean readable = false;

			// TODO
			for (int priority = 0; priority < 2; priority++) {
				Tpp tpp = new Tpp(tpg.getTopic(), tpg.getPartition(), priority == 0 ? true : false);
				Long writeOffset = findTppWriteOffset(tpp);
				if (writeOffset != null) {
					if (watcher.isReadable(writeOffset, priority)) {
						readable = true;
						break;
					}
				}
			}

			if (readable) {
				iter.remove();
				watcher.getRunnable().run();
			}

		}
	}

	private Long findTppWriteOffset(Tpp tpp) {
		return m_writeOffsets.get(tpp);
	}

	@Override
	public void select() {
		new Thread() {
			public void run() {
				while (!Thread.interrupted()) {
					try {
						m_tasks.take().run();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}

			}
		}.start();
	}

}
