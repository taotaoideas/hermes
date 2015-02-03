package com.ctrip.hermes.message.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.MessageContext;
import com.ctrip.hermes.message.MessageSink;
import com.ctrip.hermes.message.codec.Codec;
import com.ctrip.hermes.message.codec.CodecManager;

public class MemoryMessageSink implements MessageSink {

	public static final String ID = "memory";

	@Inject
	private CodecManager m_codecManager;

	public MemoryMessageSink() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void handle(MessageContext ctx) {
		// TODO Auto-generated method stub
		String topic = ctx.getMessage().getTopic();
		Codec codec = m_codecManager.getCodec(topic);
		Object body = ctx.getMessage().getBody();

		System.out.println("Sending: " + new String(codec.encode(body)) + " to memory sink");

	}

}
