package com.ctrip.hermes.consumer;

import com.ctrip.hermes.storage.storage.Offset;

// TODO extends com.ctrip.hermes.message and rename and remove duplicate fields
public class Message<T> extends com.ctrip.hermes.message.Message<T> {

	private com.ctrip.hermes.storage.message.Message m_storageMsg;

	private boolean m_success = true;

	public Message(T body, com.ctrip.hermes.storage.message.Message msg) {
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
