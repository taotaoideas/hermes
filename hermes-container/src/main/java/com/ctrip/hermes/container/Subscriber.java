package com.ctrip.hermes.container;

import com.ctrip.hermes.consumer.Consumer;

public class Subscriber {

	private String m_groupId;

	private String m_topicPattern;

	private Class<? extends Consumer> m_consumerClass;

	public Subscriber(String groupId, String topicPattern, Class<? extends Consumer> consumerClass) {
		m_groupId = groupId;
		m_topicPattern = topicPattern;
		m_consumerClass = consumerClass;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public String getTopicPattern() {
		return m_topicPattern;
	}

	public Class<? extends Consumer> getConsumerClass() {
		return m_consumerClass;
	}

}
