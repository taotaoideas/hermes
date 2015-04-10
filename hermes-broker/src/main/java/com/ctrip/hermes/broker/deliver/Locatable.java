package com.ctrip.hermes.broker.deliver;

public class Locatable {

	private Object m_id;

	private long offset;

	public Locatable(Object id, long offset) {
		m_id = id;
		this.offset = offset;
	}

	public Object getId() {
		return m_id;
	}

	public long getOffset() {
		return offset;
	}

}
