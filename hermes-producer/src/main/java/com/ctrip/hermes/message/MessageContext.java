package com.ctrip.hermes.message;

public class MessageContext {

	private MessageSink m_sink;

	private int m_index;

	private Message<Object> m_message;

	public MessageContext(Message<Object> message) {
		m_message = message;
	}

	public void setSink(MessageSink sink) {
		m_sink = sink;
	}

	public int getIndex() {
		return m_index;
	}

	public void setIndex(int index) {
		m_index = index;
	}

	public MessageSink getSink() {
		return m_sink;
	}

	public Message<Object> getMessage() {
		return m_message;
	}

}
