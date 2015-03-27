package com.ctrip.hermes.core.message;

import java.util.HashMap;
import java.util.Map;

public class ProducerMessage<T> {
	private String m_topic;

	private T m_body;

	private String m_key;

	private boolean m_priority = false;

	private String m_partition;

	private int m_partitionNo;

	private int m_msgSeqNo;

	private long m_bornTime;

	private Map<String, Object> m_appProperties = new HashMap<String, Object>();

	private Map<String, Object> m_sysProperties = new HashMap<String, Object>();

	public ProducerMessage() {

	}

	public ProducerMessage(String m_topic, T m_body) {
		this.m_topic = m_topic;
		this.m_body = m_body;
	}

	public ProducerMessage(ProducerMessage<T> other) {
		setTopic(other.getTopic());
		setBody(other.getBody());
		setKey(other.getKey());
		setPriority(other.isPriority());
		setPartition(other.getPartition());
		setAppProperties(other.getAppProperties());
		setSysProperties(other.getSysProperties());
		setBornTime(other.getBornTime());
	}

	public int getMsgSeqNo() {
		return m_msgSeqNo;
	}

	public void setMsgSeqNo(int msgSeqNo) {
		m_msgSeqNo = msgSeqNo;
	}

	public int getPartitionNo() {
		return m_partitionNo;
	}

	public void setPartitionNo(int partitionNo) {
		m_partitionNo = partitionNo;
	}

	public T getBody() {
		return m_body;
	}

	public String getKey() {
		return m_key;
	}

	public String getTopic() {
		return m_topic;
	}

	@SuppressWarnings("unchecked")
	public void setBody(Object object) {
		m_body = (T) object;
	}

	public void setKey(String key) {
		m_key = key;
	}

	public boolean isPriority() {
		return m_priority;
	}

	public void setPriority(boolean priority) {
		m_priority = priority;
	}

	public String getPartition() {
		return m_partition;
	}

	public void setPartition(String partition) {
		m_partition = partition;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public long getBornTime() {
		return m_bornTime;
	}

	public void setBornTime(long bornTime) {
		m_bornTime = bornTime;
	}

	public Map<String, Object> getAppProperties() {
		return m_appProperties;
	}

	public void setAppProperties(Map<String, Object> properties) {
		m_appProperties = properties;
	}

	public void addAppProperty(String name, Object value) {
		m_appProperties.put(name, value);
	}

	@SuppressWarnings("unchecked")
	public <V> V getAppProperty(String name) {
		return (V) m_appProperties.get(name);
	}

	public Map<String, Object> getSysProperties() {
		return m_sysProperties;
	}

	public void setSysProperties(Map<String, Object> properties) {
		m_sysProperties = properties;
	}

	public void addSysProperty(String name, Object value) {
		m_sysProperties.put(name, value);
	}

	@SuppressWarnings("unchecked")
	public <V> V getSysProperty(String name) {
		return (V) m_sysProperties.get(name);
	}

}
