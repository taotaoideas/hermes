package com.ctrip.hermes.message;

import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.storage.Offset;

public class StoredMessage<T> extends Message<Object> {

	private boolean m_success = true;

	private Offset m_ackOffset;

	private Offset m_offset;

	public StoredMessage() {
	}

	public StoredMessage(Object body, Record r) {
		setBody(body);

		m_offset = r.getOffset();
		m_ackOffset = r.getAckOffset();
	}

	public StoredMessage(Record r, String topic) {
		setBody(r.getContent());

		setKey(r.getKey());
		setPartition(r.getPartition());
		setTopic(topic);

		m_offset = r.getOffset();
		m_ackOffset = r.getAckOffset();
	}

	@SuppressWarnings("unchecked")
   @Override
	public T getBody() {
		return (T) super.getBody();
	}

	public Offset getAckOffset() {
		return m_ackOffset;
	}

	public Offset getOffset() {
		return m_offset;
	}

	public void nack() {
		m_success = false;
	}

	public boolean isSuccess() {
		return m_success;
	}

	public void setSuccess(boolean success) {
		m_success = success;
	}

	public void setAckOffset(Offset ackOffset) {
		m_ackOffset = ackOffset;
	}

	public void setOffset(Offset offset) {
		m_offset = offset;
	}

}
