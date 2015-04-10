package com.ctrip.hermes.broker.deliver;

import java.util.ArrayList;
import java.util.List;

public class EnumRange<T> {

	private T m_id;

	private List<Long> m_offsets;

	public EnumRange(T id) {
		m_id = id;
		m_offsets = new ArrayList<>();
	}

	public EnumRange(T id, List<Long> offsets) {
		m_id = id;
		m_offsets = offsets;
	}

	public void addOffset(long offset) {
		m_offsets.add(offset);
	}

	public T getId() {
		return m_id;
	}

	public List<Long> getOffsets() {
		return m_offsets;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_id == null) ? 0 : m_id.hashCode());
		result = prime * result + ((m_offsets == null) ? 0 : m_offsets.hashCode());
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
		EnumRange<?> other = (EnumRange<?>) obj;
		if (m_id == null) {
			if (other.m_id != null)
				return false;
		} else if (!m_id.equals(other.m_id))
			return false;
		if (m_offsets == null) {
			if (other.m_offsets != null)
				return false;
		} else if (!m_offsets.equals(other.m_offsets))
			return false;
		return true;
	}

}
