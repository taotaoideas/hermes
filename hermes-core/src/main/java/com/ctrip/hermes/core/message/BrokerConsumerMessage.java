package com.ctrip.hermes.core.message;

import java.util.Map;

import com.ctrip.hermes.core.transport.command.MessageAckCommand;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BrokerConsumerMessage<T> implements ConsumerMessage<T> {

	private BaseConsumerMessage<T> m_baseMsg;

	private long m_msgSeq;

	private int m_partition;

	private boolean m_priority;

	private EndpointChannel m_channel;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public BrokerConsumerMessage(BaseConsumerMessage baseMsg) {
		m_baseMsg = baseMsg;
	}

	public void setChannel(EndpointChannel channel) {
		m_channel = channel;
	}

	public boolean isPriority() {
		return m_priority;
	}

	public void setPriority(boolean priority) {
		m_priority = priority;
	}

	public int getPartition() {
		return m_partition;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}

	public long getMsgSeq() {
		return m_msgSeq;
	}

	public void setMsgSeq(long msgSeq) {
		this.m_msgSeq = msgSeq;
	}

	@Override
	public void nack() {
		if (m_baseMsg.nack()) {
			MessageAckCommand cmd = new MessageAckCommand();
			cmd.addNackMsg(getTopic(), getPartition(), m_priority, m_msgSeq);
			m_channel.writeCommand(cmd);
		}
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
	public void ack() {
		if (m_baseMsg.ack()) {
			MessageAckCommand cmd = new MessageAckCommand();
			cmd.addAckMsg(getTopic(), getPartition(), m_priority, m_msgSeq);
			m_channel.writeCommand(cmd);
		}
	}

	@Override
	public MessageStatus getStatus() {
		return m_baseMsg.getStatus();
	}

}
