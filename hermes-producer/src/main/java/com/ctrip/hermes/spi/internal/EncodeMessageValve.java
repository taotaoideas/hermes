package com.ctrip.hermes.spi.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.ProducerMessage;
import com.ctrip.hermes.message.codec.MessageCodec;
import com.ctrip.hermes.spi.Valve;

public class EncodeMessageValve implements Valve {

	public static final String ID = "encode-message";

	@Inject
	private MessageCodec m_msgCodec;

	@SuppressWarnings("unchecked")
	@Override
	public void handle(PipelineContext<?> ctx, Object payload) {
		ProducerMessage<Object> msg = (ProducerMessage<Object>) payload;

		ctx.next(m_msgCodec.encode(msg));
	}

}
