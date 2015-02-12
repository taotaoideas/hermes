package com.ctrip.hermes.engine;

import com.ctrip.hermes.consumer.Consumer;

@SuppressWarnings("rawtypes")
public class Subscriber {

	private String m_groupId;

	private String m_topicPattern;

	private Consumer m_consumer;

	public Subscriber(String m_topicPattern, String m_groupId, Consumer m_consumer) {
		this.m_topicPattern = m_topicPattern;
		this.m_groupId = m_groupId;
		this.m_consumer = m_consumer;
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

}
