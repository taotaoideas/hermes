package com.ctrip.hermes.storage.message;

import java.util.HashMap;
import java.util.Map;

import com.ctrip.hermes.message.ProducerMessage;
import com.ctrip.hermes.storage.storage.Locatable;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.util.StringUtil;

public class Record implements Locatable {

	private static final int DEFAULT_PRIORITY = -1;

	private byte[] m_content;

	private Map<String, Object> m_properties = new HashMap<String, Object>();

	private Offset m_offset;

	private Offset m_ackOffset;

	public Record(ProducerMessage<byte[]> msg) {
		setContent(msg.getBody());
		setPartition(msg.getPartition());
		setPriority(msg.isPriority() ? 0 : 1);
		setKey(msg.getKey());
		setBornTime(msg.getBornTime());

		for (Map.Entry<String, Object> entry : msg.getAppProperties().entrySet()) {
			setProperty(entry.getKey(), entry.getValue());
		}
	}

	public Record() {
	}

	public Offset getAckOffset() {
		return m_ackOffset == null ? m_offset : m_ackOffset;
	}

	public void setAckOffset(Offset ackOffset) {
		m_ackOffset = ackOffset;
	}

	public Offset getOffset() {
		return m_offset;
	}

	public void setOffset(Offset offset) {
		m_offset = offset;
	}

	public byte[] getContent() {
		return m_content;
	}

	public void setContent(byte[] content) {
		m_content = content;
	}

	public void setProperty(String key, Object value) {
		m_properties.put(key, value);
	}

	@SuppressWarnings("unchecked")
	public <T> T getProperty(String key) {
		return (T) m_properties.get(key);
	}

	public Map<String, Object> getProperties() {
		return m_properties;
	}

	public void setProperties(Map<String, Object> properties) {
		m_properties = properties;
	}

	public void setPriority(int priority) {
		setProperty(MessageConstants.PROP_PRIORITY, Integer.toString(priority));
	}

	public int getPriority() {
		return StringUtil.safeToInt((String) getProperty(MessageConstants.PROP_PRIORITY), DEFAULT_PRIORITY);
	}

	public void setPartition(String partition) {
		setProperty(MessageConstants.PROP_PARTITION, partition);
	}

	public String getPartition() {
		return (String) getProperty(MessageConstants.PROP_PARTITION);
	}

	public void setKey(String key) {
		setProperty(MessageConstants.PROP_KEY, key);
	}

	public String getKey() {
		return (String) getProperty(MessageConstants.PROP_KEY);
	}

	public void setBornTime(long time) {
		setProperty(MessageConstants.PROP_BORNTIME, time);
	}

	public Long getBornTime() {
		return (Long) getProperty(MessageConstants.PROP_BORNTIME);
	}
}
