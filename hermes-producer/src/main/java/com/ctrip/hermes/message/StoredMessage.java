package com.ctrip.hermes.message;

import java.util.Map;

import com.ctrip.hermes.producer.ProducerMessage;
import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.storage.Offset;

public class StoredMessage<T> extends ProducerMessage<T> implements Message<T> {

	private boolean m_success = true;

	private Offset m_ackOffset;

	private Offset m_offset;

	public StoredMessage() {
	}

	public StoredMessage(T body) {
		setBody(body);
	}

	public StoredMessage(ProducerMessage<T> msg) {
		super(msg);
	}

	public StoredMessage(T body, Record r) {
		setBody(body);

		m_offset = r.getOffset();
		m_ackOffset = r.getAckOffset();
	}

	public StoredMessage(Record r, String topic) {
		setBody((T) r.getContent());

		setKey(r.getKey());
		setPartition(r.getPartition());
		setTopic(topic);

		for (Map.Entry<String, Object> entry : r.getProperties().entrySet()) {
			// TODO remove key, partition, topic
		}

		m_offset = r.getOffset();
		m_ackOffset = r.getAckOffset();
	}

	public Offset getAckOffset() {
		return m_ackOffset;
	}

	public Offset getOffset() {
		return m_offset;
	}

	@Override
	public void nack() {
		m_success = false;
	}

	public boolean isSuccess() {
		return m_success;
	}

	public void setSuccess(boolean success) {
		m_success = success;
	}

	public void setAckOffset(Offset ackOffset) {
		m_ackOffset = ackOffset;
	}

	public void setOffset(Offset offset) {
		m_offset = offset;
	}

	/* (non-Javadoc)
	 * @see com.ctrip.hermes.message.Message#getProperty(java.lang.String)
	 */
   @Override
   public <V> V getProperty(String name) {
	   // TODO Auto-generated method stub
	   return null;
   }

	/* (non-Javadoc)
	 * @see com.ctrip.hermes.message.Message#getProperties()
	 */
   @Override
   public Map<String, Object> getProperties() {
	   // TODO Auto-generated method stub
	   return null;
   }

}
