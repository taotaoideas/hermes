package com.ctrip.hermes.core.message;

import java.util.Iterator;

import com.ctrip.hermes.core.transport.command.AckMessageCommand;
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

	private boolean m_resend = false;

	private String m_groupId;

	private long m_correlationId;

	private EndpointChannel m_channel;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public BrokerConsumerMessage(BaseConsumerMessage baseMsg) {
		m_baseMsg = baseMsg;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public void setGroupId(String groupId) {
		m_groupId = groupId;
	}

	public long getCorrelationId() {
		return m_correlationId;
	}

	public void setCorrelationId(long correlationId) {
		m_correlationId = correlationId;
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
			AckMessageCommand cmd = new AckMessageCommand();
			cmd.getHeader().setCorrelationId(m_correlationId);
			cmd.addNackMsg(getTopic(), getPartition(), m_priority, m_groupId, m_resend, m_msgSeq);
			m_channel.writeCommand(cmd);
		}
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
		if (m_baseMsg.ack()) {
			AckMessageCommand cmd = new AckMessageCommand();
			cmd.getHeader().setCorrelationId(m_correlationId);
			cmd.addAckMsg(getTopic(), getPartition(), m_priority, m_groupId, m_resend, m_msgSeq);
			m_channel.writeCommand(cmd);
		}
	}

	@Override
	public MessageStatus getStatus() {
		return m_baseMsg.getStatus();
	}

	public void setResend(boolean resend) {
		m_resend = resend;
	}

	public boolean isResend() {
		return m_resend;
	}

}
