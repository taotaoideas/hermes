package com.ctrip.hermes.storage.range;

import java.util.Arrays;
import java.util.List;

import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.storage.Offset;

public class OffsetRecord {

	private List<Offset> m_toBeDone;

	private Offset m_toUpdate;

	private Ack m_ack;

	public OffsetRecord() {
	}

	public OffsetRecord(List<Offset> toBeDone, Offset toUpdate) {
		m_toBeDone = toBeDone;
		m_toUpdate = toUpdate;
	}

	public OffsetRecord(Offset toBeDone, Offset toUpdate) {
		m_toBeDone = Arrays.asList(toBeDone);
		m_toUpdate = toUpdate;
	}

	public OffsetRecord(Offset offset) {
		m_toBeDone = Arrays.asList(offset);
		m_toUpdate = offset;
	}

	public List<Offset> getToBeDone() {
		return m_toBeDone;
	}

	public Offset getToUpdate() {
		return m_toUpdate;
	}

	public boolean contains(OffsetRecord record) {
		return m_toBeDone.containsAll(record.getToBeDone());
	}

	public Ack getAck() {
		return m_ack;
	}

	public void setAck(Ack ack) {
		m_ack = ack;
	}

	public void setToBeDone(List<Offset> toBeDone) {
		m_toBeDone = toBeDone;
	}

	public void setToUpdate(Offset toUpdate) {
		m_toUpdate = toUpdate;
	}

	@Override
	public String toString() {
		return "OffsetRecord [m_toBeDone=" + m_toBeDone + ", m_toUpdate=" + m_toUpdate + ", m_ack=" + m_ack + "]";
	}

}
