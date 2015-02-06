package com.ctrip.hermes.broker.remoting;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.ConsumerChannel;
import com.ctrip.hermes.remoting.netty.MessageChannelManager;

public class StartConsumerRequest implements CommandProcessor {

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
		ConsumerChannel channel = new ConsumerChannel(ctx.getNettyCtx(), "topic1", "group1");
		m_channelManager.registerConsumerChannel(channel);
	}

}
