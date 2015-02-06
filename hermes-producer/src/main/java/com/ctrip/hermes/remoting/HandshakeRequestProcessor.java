package com.ctrip.hermes.remoting;

import java.util.Arrays;
import java.util.List;

public class HandshakeRequestProcessor implements CommandProcessor {

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
