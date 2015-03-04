package com.ctrip.hermes.message.internal;

import java.util.Arrays;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.channel.MessageChannelManager;
import com.ctrip.hermes.channel.ProducerChannel;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.storage.message.Message;

public class MemoryMessageSink implements PipelineSink {

	public static final String ID = "memory";

	@Inject
	private MessageChannelManager m_channelManager;

	@Override
	public void handle(PipelineContext ctx, Object input) {
		byte[] encodedMsg = (byte[]) input;
		String topic = ctx.get("topic");

		ProducerChannel channel = m_channelManager.newProducerChannel(topic);

		Message msg = new Message();
		msg.setContent(encodedMsg);

		channel.send(Arrays.asList(msg));
	}

}
