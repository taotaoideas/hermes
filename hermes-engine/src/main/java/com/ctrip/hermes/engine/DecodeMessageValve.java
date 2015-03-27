package com.ctrip.hermes.engine;

import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.spi.Valve;

public class DecodeMessageValve implements Valve {

	public static final String ID = "decode-message";

	@Override
	public void handle(PipelineContext<?> ctx, Object payload) {
//		MessageContext msgCtx = (MessageContext) payload;
//		String topic = msgCtx.getTopic();
//		Codec codec = m_codecManager.getCodec(topic);
//		List<StoredMessage<byte[]>> msgs = msgCtx.getMessages();
//
//		for (StoredMessage<byte[]> storedMsg : msgs) {
//			// TODO get or bypass class info
//			storedMsg.setBody(codec.decode(storedMsg.getBody(), msgCtx.getMessageClass()));
//		}
//		ctx.put("topic", topic);
//		ctx.next(msgs);
	}

}
