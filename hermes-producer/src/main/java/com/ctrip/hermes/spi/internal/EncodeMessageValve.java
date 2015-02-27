package com.ctrip.hermes.spi.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.MessagePackage;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.codec.Codec;
import com.ctrip.hermes.message.codec.CodecManager;
import com.ctrip.hermes.spi.Valve;

public class EncodeMessageValve implements Valve {

	public static final String ID = "encode-message";

	@Inject
	private CodecManager m_codecManager;

	@SuppressWarnings("unchecked")
	@Override
	public void handle(PipelineContext ctx, Object payload) {
		Message<Object> msg = (Message<Object>) payload;
		Codec codec = m_codecManager.getCodec(msg.getTopic());

		// TODO encode all msg properties
		ctx.next(new MessagePackage(codec.encode(msg.getBody()), msg.getKey()));
	}

}
