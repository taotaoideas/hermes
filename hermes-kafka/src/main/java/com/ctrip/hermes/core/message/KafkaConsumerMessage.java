package com.ctrip.hermes.core.message;

import java.util.Map;

public class KafkaConsumerMessage<T> implements ConsumerMessage<T> {

	private BaseConsumerMessage<T> m_baseMsg;

	private long m_msgSeq;

	/**
	 * @param baseMsg
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public KafkaConsumerMessage(BaseConsumerMessage baseMsg) {
		m_baseMsg = baseMsg;
	}

	public BaseConsumerMessage<T> getBaseMsg() {
		return m_baseMsg;
	}

	public long getMsgSeq() {
		return m_msgSeq;
	}

	public void setMsgSeq(long msgSeq) {
		this.m_msgSeq = msgSeq;
	}

	@Override
	public void nack() {
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> V getProperty(String name) {
		return (V) m_baseMsg.getAppProperties().get(name);
	}

	@Override
	public Map<String, Object> getProperties() {
		return m_baseMsg.getAppProperties();
	}

	@Override
	public long getBornTime() {
		return m_baseMsg.getBornTime();
	}

	@Override
	public String getTopic() {
		return m_baseMsg.getTopic();
	}

	@Override
	public String getKey() {
		return m_baseMsg.getKey();
	}

	@Override
	public T getBody() {
		return m_baseMsg.getBody();
	}

	@Override
	public boolean isSuccess() {
		return true;
	}
}
