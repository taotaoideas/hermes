package com.ctrip.hermes.engine;

import com.ctrip.hermes.consumer.Consumer;

@SuppressWarnings("rawtypes")
public class Subscriber {

	private String m_groupId;

	private String m_topicPattern;

	private Consumer m_consumer;

	private Class<?> m_messageClass;

	public Subscriber(String topicPattern, String groupId, Consumer consumer, Class<?> messageClass) {
		m_topicPattern = topicPattern;
		m_groupId = groupId;
		m_consumer = consumer;
		m_messageClass = messageClass;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public String getTopicPattern() {
		return m_topicPattern;
	}

	public Consumer getConsumer() {
		return m_consumer;
	}

	public Class<?> getMessageClass() {
		return m_messageClass;
	}

}
