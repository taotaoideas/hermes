package com.ctrip.hermes.message;

import java.util.HashMap;
import java.util.Map;

public class Message<T> {
	private String m_topic;

	private T m_body;

	private String m_key;

	private boolean m_priority = false;

	private String m_partition;

	private long m_bornTime;

	private Map<String, Object> m_properties = new HashMap<String, Object>();

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

	public long getBornTime() {
		return m_bornTime;
	}

	public void setBornTime(long bornTime) {
		m_bornTime = bornTime;
	}

	public Map<String, Object> getProperties() {
		return m_properties;
	}

	public void setProperties(Map<String, Object> properties) {
		m_properties = properties;
	}

}
