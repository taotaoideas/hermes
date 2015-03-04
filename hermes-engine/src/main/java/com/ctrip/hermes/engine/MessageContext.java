package com.ctrip.hermes.engine;

import java.util.List;

import com.ctrip.hermes.message.StoredMessage;

public class MessageContext {

	private List<StoredMessage<byte[]>> m_messages;

	private String m_topic;

	private Class<?> m_messageClass;

	public MessageContext(String topic, List<StoredMessage<byte[]>> messages, Class<?> messageClass) {
		m_messages = messages;
		m_topic = topic;
		m_messageClass = messageClass;
	}

	public List<StoredMessage<byte[]>> getMessages() {
		return m_messages;
	}

	public String getTopic() {
		return m_topic;
	}

	public Class<?> getMessageClass() {
		return m_messageClass;
	}

}
