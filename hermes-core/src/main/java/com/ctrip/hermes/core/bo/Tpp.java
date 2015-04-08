package com.ctrip.hermes.core.bo;

import org.unidal.tuple.Triple;

/**
 * Topic-Partition-Priority Wrapper
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class Tpp {
	private Triple<String, Integer, Boolean> m_triple = new Triple<>();

	public Tpp(String topic, int partition, boolean isPriority) {
		m_triple.setFirst(topic);
		m_triple.setMiddle(partition);
		m_triple.setLast(isPriority);
	}

	public String getTopic() {
		return m_triple.getFirst();
	}

	public int getPartition() {
		return m_triple.getMiddle();
	}

	public boolean isPriority() {
		return m_triple.getLast();
	}

	public int getPriorityInt() {
		// TODO move to other place
		return isPriority() ? 0 : 1;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_triple == null) ? 0 : m_triple.hashCode());
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
		Tpp other = (Tpp) obj;
		if (m_triple == null) {
			if (other.m_triple != null)
				return false;
		} else if (!m_triple.equals(other.m_triple))
			return false;
		return true;
	}

}