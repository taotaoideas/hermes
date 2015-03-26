package com.ctrip.hermes.remoting.command;

import com.ctrip.hermes.endpoint.EndpointChannel;

public class CommandContext {

	private Command m_command;

	private EndpointChannel m_channel;

	public CommandContext(Command command, EndpointChannel channel) {
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
