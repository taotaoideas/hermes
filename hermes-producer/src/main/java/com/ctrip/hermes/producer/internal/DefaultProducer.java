package com.ctrip.hermes.producer.internal;

import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.ProducerMessage;
import com.ctrip.hermes.producer.Producer;

public class DefaultProducer extends Producer {
	@Inject
	private Pipeline<Future<SendResult>> m_pipe;

	@Override
	public DefaultHolder message(String topic, Object body) {
		return new DefaultHolder(topic, body);
	}

	class DefaultHolder implements Holder {
		private ProducerMessage<Object> m_msg;

		public DefaultHolder(String topic, Object body) {
			m_msg = new ProducerMessage<Object>();

			m_msg.setTopic(topic);
			m_msg.setBody(body);
		}

		@Override
		public Future<SendResult> send() {
			m_msg.setBornTime(System.currentTimeMillis());
			return m_pipe.put(m_msg);
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
