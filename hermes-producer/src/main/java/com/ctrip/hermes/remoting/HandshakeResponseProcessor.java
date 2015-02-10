package com.ctrip.hermes.remoting;

import java.util.Arrays;
import java.util.List;

public class HandshakeResponseProcessor implements CommandProcessor {

	public static final String ID = "handshake-response";

	@Override
	public void process(CommandContext ctx) {
	}

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.HandshakeResponse);
	}

}
