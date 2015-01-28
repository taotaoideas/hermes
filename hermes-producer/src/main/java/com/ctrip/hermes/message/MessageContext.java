package com.ctrip.hermes.message;

public class MessageContext<T> {
	private String m_topic;

	private T m_message;

	private String m_key;

	public String getKey() {
		return m_key;
	}

	public T getMessage() {
		return m_message;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setKey(String key) {
		m_key = key;
	}

	public void setMessage(T message) {
		m_message = message;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}
}
