package com.ctrip.hermes.broker.remoting;

import java.util.Arrays;
import java.util.List;

import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;

public class HandshakeRequestProcessor implements CommandProcessor {

	public static final String ID = "handshake-request";

	@Override
	public void process(CommandContext ctx) {
		System.out.println("New producer connected");

		ctx.write(new Command(CommandType.HandshakeResponse));
	}

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.HandshakeRequest);
	}

}
