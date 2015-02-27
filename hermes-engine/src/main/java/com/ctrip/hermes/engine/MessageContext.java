package com.ctrip.hermes.engine;

import java.util.List;

import com.ctrip.hermes.storage.message.Message;

public class MessageContext {

	private List<Message> m_messages;

	private String m_topic;

	private Class<?> m_messageClass;

	public MessageContext(String topic, List<Message> messages, Class<?> messageClass) {
		m_messages = messages;
		m_topic = topic;
		m_messageClass = messageClass;
	}

	public List<Message> getMessages() {
		return m_messages;
	}

	public String getTopic() {
		return m_topic;
	}

	public Class<?> getMessageClass() {
		return m_messageClass;
	}

}
