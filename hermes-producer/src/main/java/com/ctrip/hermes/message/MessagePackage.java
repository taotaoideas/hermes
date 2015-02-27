package com.ctrip.hermes.message;

public class MessagePackage {

	private byte[] m_message;

	private String m_key;

	public MessagePackage() {
	}

	public MessagePackage(byte[] message, String key) {
		m_message = message;
		m_key = key;
	}

	public void setMessage(byte[] message) {
		m_message = message;
	}

	public void setKey(String key) {
		m_key = key;
	}

	public byte[] getMessage() {
		return m_message;
	}

	public String getKey() {
		return m_key;
	}

}
