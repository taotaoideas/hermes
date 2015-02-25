package com.ctrip.hermes.broker.remoting;

import java.util.Arrays;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.broker.ConsumerChannel;
import com.ctrip.hermes.broker.remoting.netty.NettyServerHandler;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.storage.range.OffsetRecord;

public class AckRequestProcessor implements CommandProcessor {

	public static final String ID = "ack-request";

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.AckRequest);
	}

	@Override
	public void process(CommandContext ctx) {
		NettyServerHandler nettyHandler = (NettyServerHandler) ctx.getNettyHandler();
		Command cmd = ctx.getCommand();

		ConsumerChannel cc = nettyHandler.getConsumerChannel(cmd.getCorrelationId());

		OffsetRecord rec = decode(cmd.getBody());

		cc.ack(Arrays.asList(rec));
	}

	private OffsetRecord decode(byte[] body) {
		return JSON.parseObject(body, OffsetRecord.class);
	}

}
