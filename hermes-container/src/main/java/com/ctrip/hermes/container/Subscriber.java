package com.ctrip.hermes.container;

import com.ctrip.hermes.consumer.Consumer;

public class Subscriber {

	private String m_groupId;

	private String m_topicPattern;

	private Consumer<?> m_consumer;

	public Subscriber(String groupId, String topicPattern, Consumer<?> consumer) {
		m_groupId = groupId;
		m_topicPattern = topicPattern;
		m_consumer = consumer;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public String getTopicPattern() {
		return m_topicPattern;
	}

	public Consumer<?> getConsumer() {
		return m_consumer;
	}

}
