package com.ctrip.hermes.storage.storage;

// TODO make start and end type long
public class ContinuousRange implements Range {

	private String m_id;

	private Offset m_start;

	private Offset m_end;

	public ContinuousRange(Offset start) {
		this(start, start);
	}

	public ContinuousRange(String id, long start, long end) {
		m_id = id;
		m_start = new Offset(id, start);
		m_end = new Offset(id, end);
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

	@Override
	public boolean contains(Offset offset) {
		return m_id.equals(offset.getId()) //
		      && offset.getOffset() >= m_start.getOffset() //
		      && offset.getOffset() <= m_end.getOffset();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_end == null) ? 0 : m_end.hashCode());
		result = prime * result + ((m_id == null) ? 0 : m_id.hashCode());
		result = prime * result + ((m_start == null) ? 0 : m_start.hashCode());
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
		ContinuousRange other = (ContinuousRange) obj;
		if (m_end == null) {
			if (other.m_end != null)
				return false;
		} else if (!m_end.equals(other.m_end))
			return false;
		if (m_id == null) {
			if (other.m_id != null)
				return false;
		} else if (!m_id.equals(other.m_id))
			return false;
		if (m_start == null) {
			if (other.m_start != null)
				return false;
		} else if (!m_start.equals(other.m_start))
			return false;
		return true;
	}

}
