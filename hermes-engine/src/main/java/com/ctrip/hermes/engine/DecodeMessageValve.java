package com.ctrip.hermes.engine;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.consumer.Message;
import com.ctrip.hermes.message.MessagePackage;
import com.ctrip.hermes.message.PipelineContext;
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
		Codec codec = m_codecManager.getCodec(msgCtx.getTopic());
		List<com.ctrip.hermes.storage.message.Message> msgs = msgCtx.getMessages();

		List<Object> bodies = new ArrayList<>(msgs.size());
		for (com.ctrip.hermes.storage.message.Message msg : msgs) {
			MessagePackage pkg = JSON.parseObject(msg.getContent(), MessagePackage.class);
			// TODO get or bypass class info
			Message<Object> cmsg = new Message<Object>(codec.decode(pkg.getMessage(), msgCtx.getMessageClass()), msg);
			cmsg.setKey(pkg.getKey());
			bodies.add(cmsg);
		}

		ctx.put("topic", msgCtx.getTopic());
		ctx.next(bodies);
	}

}
