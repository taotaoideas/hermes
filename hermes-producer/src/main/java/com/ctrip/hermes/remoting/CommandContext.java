package com.ctrip.hermes.remoting;

import io.netty.channel.ChannelHandlerContext;

public class CommandContext {

	private Command m_cmd;

	private ChannelHandlerContext m_channelCtx;

	public CommandContext(Command cmd, ChannelHandlerContext ctx) {
		m_cmd = cmd;
		m_channelCtx = ctx;
	}

	public Command getCommand() {
		return m_cmd;
	}

	public void write(Command cmd) {
		// TODO write or writeAndFlush?
		m_channelCtx.writeAndFlush(cmd);
	}

}
