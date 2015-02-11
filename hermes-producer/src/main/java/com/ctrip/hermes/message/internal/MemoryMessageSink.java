package com.ctrip.hermes.message.internal;

import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;

public class MemoryMessageSink implements PipelineSink {

	public static final String ID = "memory";

	@Override
	public void handle(PipelineContext ctx, Object input) {
		byte[] bodyBuf = (byte[]) input;

		System.out.println("Sending: " + new String(bodyBuf) + " to memory sink");
	}

}
