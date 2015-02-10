package com.ctrip.hermes.remoting;

import com.ctrip.hermes.remoting.netty.AbstractNettyHandler;

public class CommandContext {

	private Command m_cmd;

	private AbstractNettyHandler m_nettyHandler;

	public CommandContext(Command m_cmd, AbstractNettyHandler m_nettyHandler) {
		this.m_cmd = m_cmd;
		this.m_nettyHandler = m_nettyHandler;
	}

	public Command getCommand() {
		return m_cmd;
	}

	public void write(Command cmd) {
		m_nettyHandler.writeCommand(cmd);
	}

	public AbstractNettyHandler getNettyHandler() {
		return m_nettyHandler;
	}

}
