package com.ctrip.hermes.core.message;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.core.transport.TransferCallback;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ConsumerMessageBatch {
	private String m_topic;

	private List<Long> m_msgSeqs = new ArrayList<>();

	private TransferCallback m_transferCallback;

	private ByteBuf data;

	public ConsumerMessageBatch() {
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

	public void mergeBatch(final ConsumerMessageBatch batch) {
		if (batch == null) {
			return;
		} else {
			m_msgSeqs.addAll(batch.getMsgSeqs());
			final TransferCallback originalTransferCallback = m_transferCallback;

			m_transferCallback = new TransferCallback() {

				@Override
				public void transfer(ByteBuf out) {
					if (originalTransferCallback != null) {
						originalTransferCallback.transfer(out);
					}
					if (batch.getTransferCallback() != null) {
						batch.getTransferCallback().transfer(out);
					}
				}
			};
		}
	}
}
