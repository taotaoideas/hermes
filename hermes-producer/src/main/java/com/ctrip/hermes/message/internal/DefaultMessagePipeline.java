package com.ctrip.hermes.message.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.MessageContext;
import com.ctrip.hermes.message.MessagePipeline;
import com.ctrip.hermes.message.MessageRegistry;
import com.ctrip.hermes.message.MessageSink;
import com.ctrip.hermes.message.MessageSinkManager;
import com.ctrip.hermes.message.MessageValveChain;

public class DefaultMessagePipeline implements MessagePipeline {
	@Inject
	private MessageRegistry m_registry;

	@Inject
	private MessageSinkManager m_sinkManager;

	@Override
	public void put(Message<Object> message) {
		MessageContext ctx = new MessageContext(message);
		MessageSink sink = m_sinkManager.getSink(message.getTopic());

		new MessageValveChain(m_registry.getValveList(), sink).handle(ctx);
	}

}
