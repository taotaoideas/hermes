package com.ctrip.hermes.producer.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.MessageContext;
import com.ctrip.hermes.message.MessagePipeline;
import com.ctrip.hermes.producer.Producer;

public class DefaultProducer implements Producer {
	@Inject
	private MessagePipeline m_pipe;

	@Override
	public DefaultHolder message(String topic, Object body) {
		return new DefaultHolder(topic, body);
	}

	class DefaultHolder implements Holder {
		private String m_topic;

		private Object m_message;

		private String m_key;

		public DefaultHolder(String topic, Object message) {
			m_topic = topic;
			m_message = message;
		}

		@Override
		public void send() {
			MessageContext<Object> ctx = new MessageContext<Object>();

			ctx.setTopic(m_topic);
			ctx.setMessage(m_message);
			ctx.setKey(m_key);

			m_pipe.put(ctx);
		}

		@Override
		public Holder withKey(String key) {
			m_key = key;
			return this;
		}
	}
}
