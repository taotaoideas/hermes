package com.ctrip.hermes.broker.remoting;

import java.util.Arrays;
import java.util.List;

import com.ctrip.hermes.broker.ConsumerChannel;
import com.ctrip.hermes.broker.remoting.netty.NettyServerHandler;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;

public class AckRequestProcessor implements CommandProcessor {

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.AckRequest);
	}

	@Override
	public void process(CommandContext ctx) {
		NettyServerHandler nettyHandler = (NettyServerHandler) ctx.getNettyHandler();
		Command cmd = ctx.getCommand();

		ConsumerChannel cc = nettyHandler.getConsumerChannel(cmd.getCorrelationId());
		
		cc.ack();
	}

}
