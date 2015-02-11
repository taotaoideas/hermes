package com.ctrip.hermes.container.remoting;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;

public class ConsumeRequestProcessor implements CommandProcessor {

	public static final String ID = "sonsume-request";

	@Inject
	private ConsumerBootstrap m_consumerManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.ConsumeRequest);
	}

	@Override
	public void process(CommandContext ctx) {
		Command cmd = ctx.getCommand();

		m_consumerManager.deliverMessage(cmd.getCorrelationId(), cmd.getBody());
	}

}
