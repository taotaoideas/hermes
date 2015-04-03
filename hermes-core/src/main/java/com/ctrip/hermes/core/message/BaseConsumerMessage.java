package com.ctrip.hermes.core.message;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BaseConsumerMessage<T> {
	protected long m_bornTime;

	protected String m_key;

	protected String m_topic;

	protected int m_partition;

	protected T m_body;

	protected Map<String, Object> m_appProperties = new HashMap<String, Object>();

	protected Map<String, Object> m_sysProperties = new HashMap<String, Object>();

	public long getBornTime() {
		return m_bornTime;
	}

	public void setBornTime(long bornTime) {
		m_bornTime = bornTime;
	}

	public String getKey() {
		return m_key;
	}

	public void setKey(String key) {
		m_key = key;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}

	public T getBody() {
		return m_body;
	}

	public void setBody(T body) {
		m_body = body;
	}

	public Map<String, Object> getAppProperties() {
		return m_appProperties;
	}

	public void setAppProperties(Map<String, Object> appProperties) {
		m_appProperties = appProperties;
	}

	public Map<String, Object> getSysProperties() {
		return m_sysProperties;
	}

	public void setSysProperties(Map<String, Object> sysProperties) {
		m_sysProperties = sysProperties;
	}

}
