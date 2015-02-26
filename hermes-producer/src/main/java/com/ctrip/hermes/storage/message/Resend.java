package com.ctrip.hermes.storage.message;

import java.util.List;

import com.ctrip.hermes.storage.storage.ContinuousRange;
import com.ctrip.hermes.storage.storage.Locatable;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.util.CollectionUtil;

public class Resend implements Locatable {

	private Range m_range;

	private long m_due;

	private Offset m_offset;

	public Resend(Range range, long due) {
		m_range = range;
		m_due = due;
	}

	public Resend(List<Offset> offsets, long due) {
		this(new ContinuousRange(CollectionUtil.first(offsets), CollectionUtil.last(offsets)), due);
	}

	public Range getRange() {
		return m_range;
	}

	public void setRange(Range range) {
		m_range = range;
	}

	public long getDue() {
		return m_due;
	}

	public void setDue(long due) {
		m_due = due;
	}

	@Override
	public void setOffset(Offset offset) {
		m_offset = offset;
	}

	@Override
	public Offset getOffset() {
		return m_offset;
	}

	@Override
	public String toString() {
		return "Resend [m_range=" + m_range + ", m_due=" + m_due + "]";
	}

}
