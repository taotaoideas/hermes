package com.ctrip.hermes.message;

import java.util.HashMap;
import java.util.Map;

public class MessagePackage {

	public final static String KEY = "key";

	public final static String PARTITION = "partition";

	public final static String TOPIC = "topic";

	private byte[] m_message;

	private Map<String, Object> m_headers = new HashMap<String, Object>();

	public MessagePackage() {
	}

	public MessagePackage(byte[] message, String key) {
		m_message = message;
		setKey(key);
	}

	public void setMessage(byte[] message) {
		m_message = message;
	}

	public void setKey(String key) {
		m_headers.put(KEY, key);
	}

	public byte[] getMessage() {
		return m_message;
	}

	public String getKey() {
		return (String) m_headers.get(KEY);
	}

	public void addHeader(String key, Object value) {
		m_headers.put(key, value);
	}

	public Object getHeader(String key) {
		return m_headers.get(key);
	}

	public Map<String, Object> getHeaders() {
		return m_headers;
	}

	public void setHeaders(Map<String, Object> headers) {
		m_headers = headers;
	}

}
