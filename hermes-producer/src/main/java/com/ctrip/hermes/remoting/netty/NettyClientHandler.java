package com.ctrip.hermes.remoting.netty;

import io.netty.channel.ChannelHandlerContext;

import com.ctrip.hermes.remoting.command.Command;


public class NettyClientHandler extends AbstractNettyHandler {

	private Command m_initCmd;

	public void setInitCmd(Command initCmd) {
		m_initCmd = initCmd;
	}

	@Override
	protected void doChannelActive(ChannelHandlerContext ctx) throws Exception {
		if (m_initCmd != null) {
			writeCommand(m_initCmd);
		}
	}

}
