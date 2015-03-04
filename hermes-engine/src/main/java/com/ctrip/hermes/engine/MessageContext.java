package com.ctrip.hermes.engine;

import java.util.List;

import com.ctrip.hermes.storage.message.Record;

public class MessageContext {

	private List<Record> m_messages;

	private String m_topic;

	private Class<?> m_messageClass;

	public MessageContext(String topic, List<Record> messages, Class<?> messageClass) {
		m_messages = messages;
		m_topic = topic;
		m_messageClass = messageClass;
	}

	public List<Record> getMessages() {
		return m_messages;
	}

	public String getTopic() {
		return m_topic;
	}

	public Class<?> getMessageClass() {
		return m_messageClass;
	}

}
