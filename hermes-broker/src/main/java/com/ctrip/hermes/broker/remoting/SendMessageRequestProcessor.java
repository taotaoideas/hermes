package com.ctrip.hermes.broker.remoting;

import java.util.Arrays;
import java.util.List;

import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;

public class SendMessageRequestProcessor implements CommandProcessor {

	public static final String ID = "send-message-request";

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.SendMessageRequest);
	}

	@Override
	public void process(CommandContext ctx) {
		Command cmd = ctx.getCommand();
		String topic = cmd.getHeader("topic");

		System.out.println(String.format("Saving message %s to topic %s", new String(cmd.getBody()), topic));
	}

}
