package com.ctrip.hermes.message.internal;

import java.util.List;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.MessageContext;
import com.ctrip.hermes.message.MessagePipeline;
import com.ctrip.hermes.message.MessageRegistry;
import com.ctrip.hermes.message.MessageSink;
import com.ctrip.hermes.message.MessageSinkManager;
import com.ctrip.hermes.message.MessageValveChain;
import com.ctrip.hermes.spi.MessageValve;

public class DefaultMessagePipeline implements MessagePipeline, Initializable {
	@Inject
	private MessageRegistry m_registry;

	@Inject
	private MessageSinkManager m_sinkManager;

	private MessageValveChain m_chain;

	@Override
	public void put(Message<Object> message) {
		MessageContext ctx = new MessageContext(message);
		MessageSink sink = m_sinkManager.getSink(message.getTopic());

		ctx.setSink(sink);
		m_chain.handle(ctx);
	}

	@Override
	public void initialize() throws InitializationException {
		List<MessageValve> valves = m_registry.getValveList();

		m_chain = new MessageValveChain(valves);
	}
}
