package com.ctrip.hermes.producer.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.producer.Producer;

public class DefaultProducer extends Producer {
	@Inject
	private Pipeline m_pipe;

	@Override
	public DefaultHolder message(String topic, Object body) {
		return new DefaultHolder(topic, body);
	}

	class DefaultHolder implements Holder {
		private Message<Object> m_msg;

		public DefaultHolder(String topic, Object body) {
			m_msg = new Message<Object>();

			m_msg.setTopic(topic);
			m_msg.setBody(body);
		}

		@Override
		public void send() {
			m_msg.setBornTime(System.currentTimeMillis());
			m_pipe.put(m_msg);
		}

		@Override
		public Holder withKey(String key) {
			m_msg.setKey(key);
			return this;
		}

		@Override
		public Holder withPriority() {
			m_msg.setPriority(true);
			return this;
		}

		@Override
		public Holder withPartition(String partition) {
			m_msg.setPartition(partition);
			return this;
		}
	}
}
