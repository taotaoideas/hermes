package com.ctrip.hermes.remoting.command;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class Header {
	private static AtomicLong CorrelationId = new AtomicLong(0);

	private int m_version = 1;

	private CommandType m_type;

	private long m_correlationId = CorrelationId.getAndIncrement();

	private Map<String, String> properties = new HashMap<String, String>();

	public int getVersion() {
		return m_version;
	}

	public void setVersion(int version) {
		m_version = version;
	}

	public CommandType getType() {
		return m_type;
	}

	public void setType(CommandType type) {
		m_type = type;
	}

	public long getCorrelationId() {
		return m_correlationId;
	}

	public void setCorrelationId(long correlationId) {
		m_correlationId = correlationId;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	public void addProperty(String key, String value) {
		this.properties.put(key, value);
	}

	public void parse(ByteBuffer buf){
		// TODO
	}
	
	public ByteBuffer toByteBuffer(){
		// TODO
		return null;
	}
}
