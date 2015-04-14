package com.ctrip.hermes.broker.deliver;

public class Locatable<T> {

	private T m_id;

	private long offset;

	public Locatable(T id, long offset) {
		m_id = id;
		this.offset = offset;
	}

	public T getId() {
		return m_id;
	}

	public long getOffset() {
		return offset;
	}

}
