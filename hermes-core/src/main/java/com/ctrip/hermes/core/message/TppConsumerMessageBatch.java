package com.ctrip.hermes.core.message;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.core.transport.TransferCallback;

/**
 * mapping to one <topic, partition, priority>
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class TppConsumerMessageBatch {
	private String m_topic;

	private int m_partition;

	private boolean m_priority;

	private boolean m_resend = false;

	private List<Long> m_msgSeqs = new ArrayList<>();

	private TransferCallback m_transferCallback;

	private ByteBuf data;

	public TppConsumerMessageBatch() {
	}

	public boolean isResend() {
		return m_resend;
	}

	public void setResend(boolean resend) {
		m_resend = resend;
	}

	public int getPartition() {
		return m_partition;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}

	public boolean isPriority() {
		return m_priority;
	}

	public void setPriority(boolean priority) {
		m_priority = priority;
	}

	public ByteBuf getData() {
		return data;
	}

	public void setData(ByteBuf data) {
		this.data = data;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public List<Long> getMsgSeqs() {
		return m_msgSeqs;
	}

	public void addMsgSeq(long msgSeq) {
		m_msgSeqs.add(msgSeq);
	}

	public void addMsgSeqs(List<Long> msgSeqs) {
		m_msgSeqs.addAll(msgSeqs);
	}

	public TransferCallback getTransferCallback() {
		return m_transferCallback;
	}

	public void setTransferCallback(TransferCallback transferCallback) {
		m_transferCallback = transferCallback;
	}

	public int size() {
		return m_msgSeqs.size();
	}

}
