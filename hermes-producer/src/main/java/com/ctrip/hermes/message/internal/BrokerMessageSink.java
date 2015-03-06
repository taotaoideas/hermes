package com.ctrip.hermes.message.internal;

import java.nio.ByteBuffer;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.ClientManager;
import com.ctrip.hermes.remoting.netty.NettyClientHandler;

public class BrokerMessageSink implements PipelineSink {
	public static final String ID = "broker";

	@Inject
	private ClientManager m_channelManager;

	@Override
	public void handle(PipelineContext ctx, Object input) {
		String topic = ctx.get("topic");
		ByteBuffer msgBuf = (ByteBuffer) input;

		// TODO use bytebuffer
		msgBuf.flip();
		byte[] msgBytes = new byte[msgBuf.limit()];
		msgBuf.get(msgBytes);

		NettyClientHandler client = m_channelManager.findProducerClient(topic);
		Command cmd = new Command(CommandType.SendMessageRequest) //
		      .setBody(msgBytes) //
		      .addHeader("topic", topic);

		client.writeCommand(cmd);
	}
}
