package com.ctrip.hermes.consumer.engine;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@SuppressWarnings("rawtypes")
public class ConsumerContext {
	private Topic m_topic;

	private String m_groupId;

	private Class<?> m_messageClazz;

	private Consumer m_consumer;

	public ConsumerContext(Topic topic, String groupId, Consumer consumer, Class<?> messageClazz) {
		m_topic = topic;
		m_groupId = groupId;
		m_consumer = consumer;
		m_messageClazz = messageClazz;
	}

	public Class<?> getMessageClazz() {
		return m_messageClazz;
	}

	public void setMessageClazz(Class<?> messageClazz) {
		m_messageClazz = messageClazz;
	}

	public Topic getTopic() {
		return m_topic;
	}

	public void setTopic(Topic topic) {
		m_topic = topic;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public void setGroupId(String groupId) {
		m_groupId = groupId;
	}

	public Consumer getConsumer() {
		return m_consumer;
	}

	public void setConsumer(Consumer consumer) {
		this.m_consumer = consumer;
	}

}
