package com.ctrip.hermes.message.internal;

import java.util.Arrays;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.channel.MessageChannelManager;
import com.ctrip.hermes.channel.ProducerChannel;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.codec.MessageCodec;

public class MemoryMessageSink implements PipelineSink {

	public static final String ID = "memory";

	@Inject
	private MessageChannelManager m_channelManager;

	@Inject
	private MessageCodec m_msgCodec;

	@Override
	public void handle(PipelineContext ctx, Object input) {
		byte[] encodedMsg = (byte[]) input;
		String topic = ctx.get("topic");

		ProducerChannel channel = m_channelManager.newProducerChannel(topic);

		channel.send(Arrays.asList(m_msgCodec.decode(encodedMsg)));
	}

}
