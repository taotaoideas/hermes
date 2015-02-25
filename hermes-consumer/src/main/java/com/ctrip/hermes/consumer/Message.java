package com.ctrip.hermes.consumer;

public class Message<T> {

	private T m_body;

	public Message(T body) {
		m_body = body;
	}

	public T getBody() {
		return m_body;
	}

}
