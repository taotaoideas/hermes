package com.ctrip.hermes.message.internal;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.future.FutureManager;
import com.ctrip.hermes.remoting.netty.ClientManager;
import com.ctrip.hermes.remoting.netty.NettyClientHandler;
import com.google.common.util.concurrent.SettableFuture;

public class BrokerMessageSink implements PipelineSink<Future<SendResult>> {
	public static final String ID = "broker";

	@Inject
	private ClientManager m_channelManager;

	@Inject
	private FutureManager m_futureManager;

	@Override
	public Future<SendResult> handle(PipelineContext<Future<SendResult>> ctx, Object input) {
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

		final SettableFuture<SendResult> future = m_futureManager.newFuture(cmd.getCorrelationId());

		client.writeCommand(cmd);

		return future;
	}
}
