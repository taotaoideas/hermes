package com.ctrip.hermes.message.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.MessageContext;
import com.ctrip.hermes.message.MessagePipeline;
import com.ctrip.hermes.spi.Registry;

public class DefaultMessagePipeline implements MessagePipeline {
	@Inject
	private Registry m_registry;

	@Override
	public void put(MessageContext<Object> ctx) {

	}
}
