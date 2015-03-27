package com.ctrip.hermes.core.transport.command.processor;

import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

public class CommandProcessorContext {

	private Command m_command;

	private EndpointChannel m_channel;

	public CommandProcessorContext(Command command, EndpointChannel channel) {
		m_command = command;
		m_channel = channel;
	}

	public void write(Command cmd) {
		m_channel.writeCommand(cmd);
	}

	public Command getCommand() {
		return m_command;
	}

}
