package com.ctrip.hermes.broker.remoting;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.ConsumerChannelHandler;
import com.ctrip.hermes.broker.MessageChannelManager;
import com.ctrip.hermes.broker.storage.message.Message;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;

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

		m_channelManager.newConsumerChannel(topic, groupId, new ConsumerChannelHandler() {

			@Override
			public void handle(List<Message> msgs) {
				Command consumeReq = new Command(CommandType.ConsumeRequest) //
				      .setCorrelationId(cmd.getCorrelationId()) //
				      .setBody(encode(msgs));

				ctx.getNettyCtx().writeAndFlush(consumeReq);
			}

			public boolean isOpen() {
				// TODO add listener to netty handler
				return true;
			}

		});
	}

	private byte[] encode(List<Message> msgs) {
		// TODO
		return msgs.get(0).getContent();
	}

}
