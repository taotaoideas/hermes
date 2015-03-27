package com.ctrip.hermes.core.endpoint;

import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;

public class NettyServerEndpointChannel extends NettyEndpointChannel {

	public NettyServerEndpointChannel(CommandProcessorManager cmdProcessorManager) {
		super(cmdProcessorManager);
	}

	@Override
	public void start() {
	}

}
