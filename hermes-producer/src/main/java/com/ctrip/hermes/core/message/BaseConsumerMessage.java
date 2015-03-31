package com.ctrip.hermes.core.message;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseConsumerMessage<T> implements ConsumerMessage<T> {
	protected long m_bornTime;

	protected String m_key;

	protected String m_topic;

	protected int m_partition;

	protected T m_body;

	protected Map<String, Object> m_appProperties = new HashMap<String, Object>();

	protected Map<String, Object> m_sysProperties = new HashMap<String, Object>();

	protected boolean m_priority;

	protected boolean m_success = true;

	public boolean isSuccess() {
		return m_success;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> V getProperty(String name) {
		return (V) m_appProperties.get(name);
	}

	@Override
	public Map<String, Object> getProperties() {
		return m_appProperties;
	}

	@Override
	public long getBornTime() {
		return m_bornTime;
	}

	public boolean isPriority() {
		return m_priority;
	}

	@Override
	public String getTopic() {
		return m_topic;
	}

	@Override
	public String getKey() {
		return m_key;
	}

	@Override
	public T getBody() {
		return m_body;
	}

}
