package com.ctrip.hermes.engine;

import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.message.codec.Codec;
import com.ctrip.hermes.message.codec.CodecManager;
import com.ctrip.hermes.message.codec.StoredMessageCodec;
import com.ctrip.hermes.spi.Valve;

public class DecodeMessageValve implements Valve {

	public static final String ID = "decode-message";

	@Inject
	private CodecManager m_codecManager;
	
	@Inject
	private StoredMessageCodec m_codec;

	@Override
	public void handle(PipelineContext ctx, Object payload) {
		MessageContext msgCtx = (MessageContext) payload;
		String topic = msgCtx.getTopic();
		Codec codec = m_codecManager.getCodec(topic);
		List<StoredMessage<byte[]>> msgs = msgCtx.getMessages();

		for (StoredMessage<byte[]> msg : msgs) {
			// TODO get or bypass class info
			msg.setBody(codec.decode(msg.getBody(), msgCtx.getMessageClass()));
		}

		ctx.put("topic", topic);
		ctx.next(msgs);
	}

}
