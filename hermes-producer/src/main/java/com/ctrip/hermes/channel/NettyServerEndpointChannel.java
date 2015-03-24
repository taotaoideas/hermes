package com.ctrip.hermes.channel;

import com.ctrip.hermes.remoting.command.CommandProcessorManager;

public class NettyServerEndpointChannel extends NettyEndpointChannel {

	public NettyServerEndpointChannel(CommandProcessorManager cmdProcessorManager) {
		super(cmdProcessorManager);
	}

	@Override
	public void start() {
	}

}
