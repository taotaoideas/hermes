package com.ctrip.hermes.container.remoting;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.message.codec.StoredMessageCodec;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;

public class ConsumeRequestProcessor implements CommandProcessor {

	public static final String ID = "consume-request";

	@Inject
	private ConsumerBootstrap m_bootstrap;
	
	@Inject
	private StoredMessageCodec m_codec;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.ConsumeRequest);
	}

	@Override
	public void process(CommandContext ctx) {
		Command cmd = ctx.getCommand();

		// TODO use bytebuffer
		List<StoredMessage<byte[]>> msgs = m_codec.decode(ByteBuffer.wrap(cmd.getBody()));
		m_bootstrap.deliverMessage(cmd.getCorrelationId(), msgs);
	}

}
