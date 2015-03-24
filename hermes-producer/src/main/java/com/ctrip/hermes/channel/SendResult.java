package com.ctrip.hermes.channel;

public class SendResult {

	private boolean m_success = false;

	private String m_catMessageId;

	public SendResult(boolean success, String catMessageId) {
		m_success = success;
		m_catMessageId = catMessageId;
	}

	public SendResult(boolean success) {
		this(success, null);
	}

	public String getCatMessageId() {
		return m_catMessageId;
	}

	public void setCatMessageId(String catMessageId) {
		m_catMessageId = catMessageId;
	}

	public boolean isSuccess() {
		return m_success;
	}

}
