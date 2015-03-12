package com.ctrip.hermes.broker.remoting;

import java.util.Arrays;
import java.util.List;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.broker.remoting.netty.NettyServerHandler;
import com.ctrip.hermes.channel.ConsumerChannel;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.storage.range.OffsetRecord;

public class AckRequestProcessor implements CommandProcessor, LogEnabled {

	public static final String ID = "ack-request";

	private Logger m_logger;

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

		m_logger.info("Receive ack " + rec);
		cc.ack(Arrays.asList(rec));
	}

	private OffsetRecord decode(byte[] body) {
		return JSON.parseObject(body, OffsetRecord.class);
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

}
