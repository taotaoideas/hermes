package com.ctrip.hermes.message.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.codec.Codec;
import com.ctrip.hermes.message.codec.CodecManager;

public class MemoryMessageSink implements MessagePipelineSink {

	public static final String ID = "memory";

	@Inject
	private CodecManager m_codecManager;

	public MemoryMessageSink() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void handle(PipelineContext<Message<Object>> ctx) {
		// TODO Auto-generated method stub
		String topic = ctx.getMessage().getTopic();
		Codec codec = m_codecManager.getCodec(topic);
		Object body = ctx.getMessage().getBody();

		System.out.println("Sending: " + new String(codec.encode(body)) + " to memory sink");

	}

}
