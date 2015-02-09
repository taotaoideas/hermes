package com.ctrip.hermes.remoting;

import java.util.HashMap;
import java.util.Map;

public class Command {

	private CommandType m_type;

	private Map<String, String> m_headers = new HashMap<String, String>();

	private byte[] m_body;

	public Command() {
	}

	public Command(CommandType type) {
		m_type = type;
	}

	public byte[] getBody() {
		return m_body;
	}

	public void setBody(byte[] body) {
		m_body = body;
	}

	public CommandType getType() {
		return m_type;
	}

	public void addHeader(String name, String value) {
		m_headers.put(name, value);
	}

	public Map<String, String> getHeaders() {
		return m_headers;
	}

	public void setHeaders(Map<String, String> headers) {
		m_headers = headers;
	}

	public void setType(CommandType type) {
		m_type = type;
	}

}
