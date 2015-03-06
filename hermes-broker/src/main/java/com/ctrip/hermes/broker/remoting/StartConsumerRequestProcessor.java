package com.ctrip.hermes.broker.remoting;

import io.netty.channel.Channel;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.remoting.netty.NettyServerHandler;
import com.ctrip.hermes.channel.ConsumerChannel;
import com.ctrip.hermes.channel.ConsumerChannelHandler;
import com.ctrip.hermes.channel.MessageChannelManager;
import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.message.codec.StoredMessageCodec;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.ChannelEventListener;

public class StartConsumerRequestProcessor implements CommandProcessor {

	public static final String ID = "start-consumer-requesut";

	@Inject
	private MessageChannelManager m_channelManager;

	@Inject
	private StoredMessageCodec m_codec;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.StartConsumerRequest);
	}

	@Override
	public void process(final CommandContext ctx) {
		NettyServerHandler nettyHandler = (NettyServerHandler) ctx.getNettyHandler();
		final Command cmd = ctx.getCommand();
		final String topic = cmd.getHeader("topic");
		String groupId = cmd.getHeader("groupId");
		String partition = cmd.getHeader("partition");

		final ConsumerChannel cc = m_channelManager.newConsumerChannel(topic, groupId, partition);
		nettyHandler.addConsumerChannel(cmd.getCorrelationId(), cc);

		final AtomicBoolean m_open = new AtomicBoolean(true);
		nettyHandler.addChannelEventListener(new ChannelEventListener() {

			@Override
			public void onChannelClose(Channel channel) {
				m_open.set(false);

				cc.close();
			}
		});

		cc.start(new ConsumerChannelHandler() {

			@Override
			public void handle(List<StoredMessage<byte[]>> msgs) {
				Command consumeReq = new Command(CommandType.ConsumeRequest) //
				      .setCorrelationId(cmd.getCorrelationId()) //
				      .setBody(encode(msgs));

				ctx.write(consumeReq);
			}

			public boolean isOpen() {
				return m_open.get();
			}

		});
	}

	private byte[] encode(List<StoredMessage<byte[]>> msgs) {
		ByteBuffer buf = m_codec.encode(msgs);
		buf.flip();
		byte[] bytes = new byte[buf.limit()];
		// TODO use bytebuffer
		buf.get(bytes);

		return bytes;
	}

}
