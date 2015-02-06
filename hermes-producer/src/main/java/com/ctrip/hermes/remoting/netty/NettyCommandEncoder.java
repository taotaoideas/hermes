package com.ctrip.hermes.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandCodec;

public class NettyCommandEncoder extends MessageToByteEncoder<Command> {

	@Inject
	private CommandCodec m_codec;

	@Override
	protected void encode(ChannelHandlerContext ctx, Command cmd, ByteBuf out) throws Exception {
		out.writeBytes(m_codec.encode(cmd));
	}

}
