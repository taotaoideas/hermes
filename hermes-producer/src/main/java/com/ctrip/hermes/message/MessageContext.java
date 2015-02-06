package com.ctrip.hermes.message;

import java.util.LinkedHashMap;
import java.util.Map;

public class MessageContext {

	private Message<Object> m_message;

	private Map<String, String> m_reqHeaders = new LinkedHashMap<String, String>();

	private Map<String, String> m_resHeaders = new LinkedHashMap<String, String>();

	public MessageContext(Message<Object> message) {
		m_message = message;
	}

	public Message<Object> getMessage() {
		return m_message;
	}

}
