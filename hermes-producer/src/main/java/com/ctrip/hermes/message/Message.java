package com.ctrip.hermes.message;

public class Message<T> {
	private String m_topic;

	private T m_body;

	private String m_key;

	private boolean m_priority = false;

	private String m_partition;

	public T getBody() {
		return m_body;
	}

	public String getKey() {
		return m_key;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setBody(T body) {
		m_body = body;
	}

	public void setKey(String key) {
		m_key = key;
	}

	public boolean isPriority() {
		return m_priority;
	}

	public void setPriority(boolean priority) {
		m_priority = priority;
	}

	public String getPartition() {
		return m_partition;
	}

	public void setPartition(String partition) {
		m_partition = partition;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

}
