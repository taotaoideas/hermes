package com.ctrip.hermes.message.internal;

import com.ctrip.hermes.message.MessageSink;
import com.ctrip.hermes.message.MessageSinkManager;

public class DefaultMessageSinkManager implements MessageSinkManager {

	@Override
	public MessageSink getSink(String topic) {
		// TODO
		return new MemoryMessageSink();
	}
}
