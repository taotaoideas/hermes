package com.ctrip.hermes.storage.storage.memory;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.storage.spi.Storage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Locatable;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.util.CollectionUtil;

public class MemoryStorage<T> implements Storage<T> {

	private String m_id;

	private List<T> m_contents = new ArrayList<T>();

	public MemoryStorage(String id) {
		m_id = id;
	}

	@Override
	public synchronized void append(List<T> payloads) {

		if (payloads != null) {
			int idx = m_contents.size();

			m_contents.addAll(payloads);

			for (T c : payloads) {
				System.out.println("Saving " + c + " to memory storage");
				if (c instanceof Locatable) {
					((Locatable) c).setOffset(new Offset(m_id, idx++));
				}
			}

		}

	}

	public synchronized Browser<T> createBrowser(long offset) {
		// memory storage will start from zero instead of largest offset plus 1
		// effectively treat every consumer as 'old' consumer
		long nextReadIdx = 0;

		if (offset > 0) {
			nextReadIdx = offset;
		}

		return new MemoryBrowser(nextReadIdx);
	}

	// TODO thread-safety
	class MemoryBrowser implements Browser<T> {

		private long m_nextReadIdx;

		public MemoryBrowser(long nextReadIdx) {
			m_nextReadIdx = nextReadIdx;
		}

		@Override
		public synchronized List<T> read(int batchSize) {
			List<T> result = new ArrayList<T>();
			int remain = batchSize;

			for (int i = (int) m_nextReadIdx; i < m_contents.size(); i++) {
				if (remain <= 0) {
					break;
				}

				T c = m_contents.get(i);
				attachOffset(c, i);

				result.add(c);

				remain--;
				m_nextReadIdx = i + 1;
			}

			return result;
		}

		@Override
		public synchronized void seek(long offset) {
			m_nextReadIdx = offset;
		}

	}

	private void attachOffset(T c, int i) {
		if (c instanceof Locatable) {
			((Locatable) c).setOffset(new Offset(m_id, i));
		}
	}

	@Override
	public synchronized List<T> read(Range range) {
		List<T> result = new ArrayList<T>();

		for (long i = range.startOffset().getOffset(); i <= range.endOffset().getOffset(); i++) {
			T c = m_contents.get((int) i);
			attachOffset(c, (int) i);

			result.add(c);
		}

		return result;
	}

	@Override
	public String getId() {
		return m_id;
	}

	@Override
	public synchronized T top() {
		return CollectionUtil.last(m_contents);
	}

}
