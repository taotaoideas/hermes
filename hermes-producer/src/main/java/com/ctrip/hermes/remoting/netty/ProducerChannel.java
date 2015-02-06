package com.ctrip.hermes.remoting.netty;

import com.ctrip.hermes.remoting.Command;

public class ProducerChannel {

	private NettyClientHandler m_handler;

	public ProducerChannel(NettyClientHandler handler) {
		m_handler = handler;
	}

	public void writeCommand(Command cmd) {
		m_handler.writeCommand(cmd);
	}

}
