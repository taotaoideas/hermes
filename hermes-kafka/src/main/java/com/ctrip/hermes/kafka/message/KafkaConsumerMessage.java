package com.ctrip.hermes.kafka.message;

import java.util.Iterator;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;

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
		m_baseMsg.nack();
	}

	@Override
	public String getProperty(String name) {
		return m_baseMsg.getDurableAppProperty(name);
	}

	@Override
	public Iterator<String> getPropertyNames() {
		return m_baseMsg.getRawDurableAppPropertyNames();
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
	public void ack() {
		m_baseMsg.ack();
	}

	@Override
	public MessageStatus getStatus() {
		return m_baseMsg.getStatus();
	}
}
