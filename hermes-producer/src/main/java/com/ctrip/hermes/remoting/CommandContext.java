package com.ctrip.hermes.remoting;

import io.netty.channel.ChannelHandlerContext;

public class CommandContext {

	private Command m_cmd;

	private ChannelHandlerContext m_nettyCtx;

	public CommandContext(Command cmd, ChannelHandlerContext nettyCtx) {
		m_cmd = cmd;
		m_nettyCtx = nettyCtx;
	}

	public Command getCommand() {
		return m_cmd;
	}

	public void write(Command cmd) {
		m_nettyCtx.writeAndFlush(cmd);
	}

	public ChannelHandlerContext getNettyCtx() {
		return m_nettyCtx;
	}

}
