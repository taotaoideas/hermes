package com.ctrip.hermes.broker.remoting;

import io.netty.channel.Channel;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.ConsumerChannel;
import com.ctrip.hermes.broker.ConsumerChannelHandler;
import com.ctrip.hermes.broker.MessageChannelManager;
import com.ctrip.hermes.broker.storage.message.Message;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.ChannelEventListener;

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
		final Command cmd = ctx.getCommand();
		String topic = cmd.getHeader("topic");
		String groupId = cmd.getHeader("groupId");

		final ConsumerChannel consumerChannel = m_channelManager.newConsumerChannel(topic, groupId);

		final AtomicBoolean m_open = new AtomicBoolean(true);
		ctx.getNettyHandler().addChannelEventListener(new ChannelEventListener() {

			@Override
			public void onChannelClose(Channel channel) {
				m_open.set(false);

				consumerChannel.close();
			}
		});

		consumerChannel.setHandler(new ConsumerChannelHandler() {

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

		consumerChannel.open();
	}

	private byte[] encode(List<Message> msgs) {
		// TODO
		return msgs.get(0).getContent();
	}

}
