package com.ctrip.hermes.broker.remoting;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.MessageChannelManager;

public class StartConsumerRequestProcessor implements CommandProcessor {

	public static final String ID = "start-consumer-requesut";

	@Inject
	private MessageChannelManager m_channelManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.StartConsumerRequest);
	}

	@Override
	public void process(CommandContext ctx) {
		// TODO
//		ConsumerChannel channel = new ConsumerChannel(ctx.getNettyCtx(), "topic1", "group1");
//		m_channelManager.registerConsumerChannel(channel);

		Command cmd = new Command(CommandType.ConsumeRequest) //
		      .setBody(UUID.randomUUID().toString().getBytes()) //
		      .setCorrelationId(ctx.getCommand().getCorrelationId());

		ctx.getNettyCtx().writeAndFlush(cmd);
	}

}
