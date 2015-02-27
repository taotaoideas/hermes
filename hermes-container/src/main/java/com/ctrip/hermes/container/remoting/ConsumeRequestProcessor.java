package com.ctrip.hermes.container.remoting;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.storage.message.Message;

public class ConsumeRequestProcessor implements CommandProcessor {

	public static final String ID = "sonsume-request";

	@Inject
	private ConsumerBootstrap m_bootstrap;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.ConsumeRequest);
	}

	@Override
	public void process(CommandContext ctx) {
		Command cmd = ctx.getCommand();

		// TODO parse cmd.getBody() to multiple message bytes
		List<Message> msgs = JSON.parseObject(cmd.getBody(), new TypeReference<List<Message>>() {
		}.getType());

		m_bootstrap.deliverMessage(cmd.getCorrelationId(), msgs);
	}

}
