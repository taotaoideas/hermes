package com.ctrip.hermes.remoting;

import java.util.Arrays;
import java.util.List;

public class HandshakeResponseProcessor implements CommandProcessor {

	@Override
	public void process(CommandContext ctx) {
		System.out.println("Connected to broker");
	}

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.HandshakeResponse);
	}

}
