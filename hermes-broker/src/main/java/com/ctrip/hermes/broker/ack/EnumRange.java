package com.ctrip.hermes.broker.ack;

import java.util.ArrayList;
import java.util.List;

public class EnumRange {

	private List<Long> m_offsets;

	public EnumRange() {
		m_offsets = new ArrayList<>();
	}

	public EnumRange(List<Long> offsets) {
		m_offsets = offsets;
	}

	public void addOffset(long offset) {
		m_offsets.add(offset);
	}

	public List<Long> getOffsets() {
		return m_offsets;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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
		EnumRange other = (EnumRange) obj;
		if (m_offsets == null) {
			if (other.m_offsets != null)
				return false;
		} else if (!m_offsets.equals(other.m_offsets))
			return false;
		return true;
	}

}
