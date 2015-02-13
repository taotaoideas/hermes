package com.ctrip.hermes.engine;

import java.util.List;

public class MessageContext {

	private List<byte[]> m_messages;

	private String m_topic;

	public MessageContext(String topic, List<byte[]> messages) {
		m_topic = topic;
		m_messages = messages;
	}

	public String getTopic() {
		return m_topic;
	}

	public List<byte[]> getMessages() {
		return m_messages;
	}

}
