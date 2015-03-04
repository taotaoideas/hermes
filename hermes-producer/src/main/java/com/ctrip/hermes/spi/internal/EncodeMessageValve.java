package com.ctrip.hermes.spi.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.codec.MessageCodec;
import com.ctrip.hermes.spi.Valve;

public class EncodeMessageValve implements Valve {

	public static final String ID = "encode-message";

	@Inject
	private MessageCodec m_msgCodec;

	@SuppressWarnings("unchecked")
	@Override
	public void handle(PipelineContext ctx, Object payload) {
		Message<Object> msg = (Message<Object>) payload;

		ctx.next(m_msgCodec.encode(msg));
	}

}
