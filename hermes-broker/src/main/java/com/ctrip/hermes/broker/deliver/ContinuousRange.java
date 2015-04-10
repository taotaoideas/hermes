package com.ctrip.hermes.broker.deliver;

public class ContinuousRange<T> {

	private T m_id;

	private long m_start;

	private long m_end;

	public ContinuousRange(T id, long start, long end) {
		m_id = id;
		m_start = start;
		m_end = end;
	}

	public T getId() {
		return m_id;
	}

	public long getStart() {
		return m_start;
	}

	public long getEnd() {
		return m_end;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (m_end ^ (m_end >>> 32));
		result = prime * result + ((m_id == null) ? 0 : m_id.hashCode());
		result = prime * result + (int) (m_start ^ (m_start >>> 32));
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
		ContinuousRange<?> other = (ContinuousRange<?>) obj;
		if (m_end != other.m_end)
			return false;
		if (m_id == null) {
			if (other.m_id != null)
				return false;
		} else if (!m_id.equals(other.m_id))
			return false;
		if (m_start != other.m_start)
			return false;
		return true;
	}

}
