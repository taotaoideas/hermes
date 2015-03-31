package com.ctrip.hermes.core.message;

import java.util.Map;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BrokerConsumerMessage<T> implements ConsumerMessage<T> {

	private BaseConsumerMessage<T> m_baseMsg;

	private long m_msgSeq;

	/**
	 * @param baseMsg
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
   public BrokerConsumerMessage(BaseConsumerMessage baseMsg) {
		m_baseMsg = baseMsg;
	}

	public BaseConsumerMessage<T> getBaseMsg() {
		return m_baseMsg;
	}

	public void setBaseMsg(BaseConsumerMessage<T> baseMsg) {
		m_baseMsg = baseMsg;
	}

	public long getMsgSeq() {
		return m_msgSeq;
	}

	public void setMsgSeq(long msgSeq) {
		this.m_msgSeq = msgSeq;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.core.message.ConsumerMessage#nack()
	 */
	@Override
	public void nack() {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.core.message.ConsumerMessage#getProperty(java.lang.String)
	 */
	@Override
	public <V> V getProperty(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.core.message.ConsumerMessage#getProperties()
	 */
	@Override
	public Map<String, Object> getProperties() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.core.message.ConsumerMessage#getBornTime()
	 */
	@Override
	public long getBornTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.core.message.ConsumerMessage#getTopic()
	 */
	@Override
	public String getTopic() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.core.message.ConsumerMessage#getKey()
	 */
	@Override
	public String getKey() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.core.message.ConsumerMessage#getBody()
	 */
	@Override
	public T getBody() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.core.message.ConsumerMessage#isPriority()
	 */
	@Override
	public boolean isPriority() {
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.core.message.ConsumerMessage#isSuccess()
	 */
	@Override
	public boolean isSuccess() {
		// TODO Auto-generated method stub
		return false;
	}

}
