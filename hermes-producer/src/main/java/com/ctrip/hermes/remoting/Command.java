package com.ctrip.hermes.remoting;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Command {

	private static AtomicLong CorrelationId = new AtomicLong(0);

	private int m_version;

	private CommandType m_type;

	private long m_correlationId = CorrelationId.getAndIncrement();

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

	public Command setBody(byte[] body) {
		m_body = body;

		return this;
	}

	public CommandType getType() {
		return m_type;
	}

	public Command addHeader(String name, String value) {
		m_headers.put(name, value);

		return this;
	}

	public String getHeader(String name) {
		return m_headers.get(name);
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

	public int getVersion() {
		return m_version;
	}

	public void setVersion(int version) {
		m_version = version;
	}

	public long getCorrelationId() {
		return m_correlationId;
	}

	public Command setCorrelationId(long correlationId) {
		m_correlationId = correlationId;

		return this;
	}

}
