package com.ctrip.hermes.consumer;

import com.ctrip.hermes.storage.storage.Offset;

public class Message<T> {

	private T m_body;

	private com.ctrip.hermes.storage.message.Message m_storageMsg;

	private boolean success = true;

	public Message(T body, com.ctrip.hermes.storage.message.Message msg) {
		m_body = body;
		m_storageMsg = msg;
	}

	public T getBody() {
		return m_body;
	}

	public Offset getAckOffset() {
		return m_storageMsg.getAckOffset();
	}

	public Offset getOffset() {
		return m_storageMsg.getOffset();
	}

	public void nack() {
		success = false;
	}

	public boolean isSuccess() {
		return success;
	}

}
