package com.ctrip.hermes.storage.storage;

public class ContinuousRange implements Range {

	private String m_id;

	private Offset m_start;

	private Offset m_end;

	public ContinuousRange(Offset start) {
		this(start, start);
	}

	public ContinuousRange(Offset start, Offset end) {
		if (!start.getId().equals(end.getId())) {
			throw new RuntimeException("Offset with different id found in Range");
		}

		m_id = start.getId();
		m_start = start;
		m_end = end;
	}

	public void setId(String id) {
		m_id = id;
	}

	@Override
	public String getId() {
		return m_id;
	}

	@Override
	public Offset getStartOffset() {
		return m_start;
	}

	@Override
	public Offset getEndOffset() {
		return m_end;
	}

	@Override
	public String toString() {
		return "ContinuousRange [m_id=" + m_id + ", m_start=" + m_start + ", m_end=" + m_end + "]";
	}

	@Override
	public void setStartOffset(Offset offset) {
		m_start = offset;
	}

	@Override
	public void setEndOffset(Offset offset) {
		m_end = offset;
	}

}
