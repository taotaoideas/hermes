package com.ctrip.hermes.message;

public class Message<T> {
	private String m_topic;

	private T m_body;

	private String m_key;

	public String getKey() {
		return m_key;
	}

	public T getBody() {
		return m_body;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setKey(String key) {
		m_key = key;
	}

	public void setBody(T body) {
		m_body = body;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}
}
