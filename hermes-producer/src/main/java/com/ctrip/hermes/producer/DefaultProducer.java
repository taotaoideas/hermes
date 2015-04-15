package com.ctrip.hermes.producer;

import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.pipeline.Pipeline;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.build.BuildConstants;

@Named(type = Producer.class)
public class DefaultProducer extends Producer {
	@Inject(BuildConstants.PRODUCER)
	private Pipeline<Future<SendResult>> m_pipeline;

	@Override
	public DefaultMessageHolder message(String topic, Object body) {
		return new DefaultMessageHolder(topic, body);
	}

	class DefaultMessageHolder implements MessageHolder {
		private ProducerMessage<Object> m_msg;

		public DefaultMessageHolder(String topic, Object body) {
			m_msg = new ProducerMessage<Object>(topic, body);
		}

		@Override
		public Future<SendResult> send() {
			m_msg.setBornTime(System.currentTimeMillis());
			return m_pipeline.put(m_msg);
		}

		@Override
		public MessageHolder withKey(String key) {
			m_msg.setKey(key);
			return this;
		}

		@Override
		public MessageHolder withPriority() {
			m_msg.setPriority(true);
			return this;
		}

		@Override
		public MessageHolder withPartition(String partition) {
			m_msg.setPartition(partition);
			return this;
		}

		@Override
		public MessageHolder addProperty(String key, String value) {
			m_msg.addDurableAppProperty(key, value);
			return this;
		}
	}
}
