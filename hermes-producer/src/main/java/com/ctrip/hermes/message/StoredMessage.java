package com.ctrip.hermes.message;

import com.ctrip.hermes.storage.storage.Offset;

public class StoredMessage<T> extends Message<T> {

	private com.ctrip.hermes.storage.message.Record m_storageMsg;

	private boolean m_success = true;

	public StoredMessage(T body, com.ctrip.hermes.storage.message.Record msg) {
		setBody(body);
		m_storageMsg = msg;
	}

	public Offset getAckOffset() {
		return m_storageMsg.getAckOffset();
	}

	public Offset getOffset() {
		return m_storageMsg.getOffset();
	}

	public void nack() {
		m_success = false;
	}

	public boolean isSuccess() {
		return m_success;
	}

}
