package com.ctrip.hermes.broker.remoting;

import io.netty.channel.Channel;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.ConsumerChannel;
import com.ctrip.hermes.broker.ConsumerChannelHandler;
import com.ctrip.hermes.broker.MessageChannelManager;
import com.ctrip.hermes.broker.remoting.netty.NettyServerHandler;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.ChannelEventListener;
import com.ctrip.hermes.storage.message.Message;

public class StartConsumerRequestProcessor implements CommandProcessor {

	public static final String ID = "start-consumer-requesut";

	@Inject
	private MessageChannelManager m_channelManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.StartConsumerRequest);
	}

	@Override
	public void process(final CommandContext ctx) {
		NettyServerHandler nettyHandler = (NettyServerHandler) ctx.getNettyHandler();
		final Command cmd = ctx.getCommand();
		String topic = cmd.getHeader("topic");
		String groupId = cmd.getHeader("groupId");

		final ConsumerChannel cc = m_channelManager.newConsumerChannel(topic, groupId);
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
			public void handle(List<Message> msgs) {
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

	private byte[] encode(List<Message> msgs) {
		// TODO
		return msgs.get(0).getContent();
	}

}
