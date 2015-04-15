package com.ctrip.hermes.core.message;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.hermes.core.message.ConsumerMessage.MessageStatus;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BaseConsumerMessage<T> {
	protected long m_bornTime;

	protected String m_key;

	protected String m_topic;

	protected T m_body;

	protected PropertiesHolder m_propertiesHolder = new PropertiesHolder();

	protected AtomicReference<MessageStatus> m_status = new AtomicReference<>(MessageStatus.NOT_SET);

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

	public T getBody() {
		return m_body;
	}

	public void setBody(T body) {
		m_body = body;
	}

	public MessageStatus getStatus() {
		return m_status.get();
	}

	public boolean ack() {
		return m_status.compareAndSet(MessageStatus.NOT_SET, MessageStatus.SUCCESS);
	}

	public boolean nack() {
		return m_status.compareAndSet(MessageStatus.NOT_SET, MessageStatus.FAIL);
	}

	public void setPropertiesHolder(PropertiesHolder propertiesHolder) {
		m_propertiesHolder = propertiesHolder;
	}

	public void addDurableAppProperty(String name, String value) {
		m_propertiesHolder.addDurableAppProperty(name, value);
	}

	public void addDurableSysProperty(String name, String value) {
		m_propertiesHolder.addDurableSysProperty(name, value);
	}

	public String getDurableAppProperty(String name) {
		return m_propertiesHolder.getDurableAppProperty(name);
	}

	public String getDurableSysProperty(String name) {
		return m_propertiesHolder.getDurableSysProperty(name);
	}

	public void addVolatileProperty(String name, String value) {
		m_propertiesHolder.addVolatileProperty(name, value);
	}

	public String getVolatileProperty(String name) {
		return m_propertiesHolder.getVolatileProperty(name);
	}

	public Iterator<String> getRawDurableAppPropertyNames() {
		return m_propertiesHolder.getRawDurableAppPropertyNames().iterator();
	}

}
