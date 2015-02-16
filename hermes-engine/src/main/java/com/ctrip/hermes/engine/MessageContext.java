package com.ctrip.hermes.engine;

import java.util.List;

import com.ctrip.hermes.storage.message.Message;

public class MessageContext {

	private List<Message> m_messages;

	private String m_topic;

	public MessageContext(String topic, List<Message> messages) {
		m_messages = messages;
		m_topic = topic;
	}

	public List<Message> getMessages() {
		return m_messages;
	}

	public String getTopic() {
		return m_topic;
	}

}
