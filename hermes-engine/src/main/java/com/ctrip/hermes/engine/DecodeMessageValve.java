package com.ctrip.hermes.engine;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.message.codec.Codec;
import com.ctrip.hermes.message.codec.CodecManager;
import com.ctrip.hermes.spi.Valve;

public class DecodeMessageValve implements Valve {

	public static final String ID = "decode-message";

	@Inject
	private CodecManager m_codecManager;

	@Override
	public void handle(PipelineContext ctx, Object payload) {
		MessageContext msgCtx = (MessageContext) payload;
		String topic = msgCtx.getTopic();
		Codec codec = m_codecManager.getCodec(topic);
		List<com.ctrip.hermes.storage.message.Record> msgs = msgCtx.getMessages();

		List<Object> bodies = new ArrayList<>(msgs.size());
		for (com.ctrip.hermes.storage.message.Record msg : msgs) {
			// TODO get or bypass class info
			StoredMessage<Object> cmsg = new StoredMessage<Object>(codec.decode(msg.getContent(), msgCtx.getMessageClass()), msg);
			cmsg.setKey(msg.getKey());
			cmsg.setTopic(topic);

			bodies.add(cmsg);
		}

		ctx.put("topic", topic);
		ctx.next(bodies);
	}

}
