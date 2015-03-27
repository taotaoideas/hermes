package com.ctrip.hermes.container.remoting;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.engine.ConsumerBootstrap;

public class ConsumeRequestProcessor implements CommandProcessor {

	public static final String ID = "consume-request";

	@Inject
	private ConsumerBootstrap m_bootstrap;
	
//	@Inject
//	private StoredMessageCodec m_codec;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList();
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		//		Command cmd = ctx.getCommand();

		// TODO use bytebuffer
//		List<StoredMessage<byte[]>> msgs = m_codec.decode(ByteBuffer.wrap(cmd.getBody()));
//		m_bootstrap.deliverMessage(cmd.getCorrelationId(), msgs);
	}

}
