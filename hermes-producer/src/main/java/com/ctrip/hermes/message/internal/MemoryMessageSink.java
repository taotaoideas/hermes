package com.ctrip.hermes.message.internal;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.channel.MessageChannelManager;
import com.ctrip.hermes.channel.ProducerChannel;
import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.codec.MessageCodec;

public class MemoryMessageSink implements PipelineSink<Future<SendResult>> {

	public static final String ID = "memory";

	@Inject
	private MessageChannelManager m_channelManager;

	@Inject
	private MessageCodec m_msgCodec;

	@Override
	public Future<SendResult> handle(PipelineContext<Future<SendResult>> ctx, Object input) {
		byte[] encodedMsg = (byte[]) input;
		String topic = ctx.get("topic");

		ProducerChannel channel = m_channelManager.newProducerChannel(topic);

		List<SendResult> result = channel.send(Arrays.asList(m_msgCodec.decode(ByteBuffer.wrap(encodedMsg))));
		
		return null;
	}

}
