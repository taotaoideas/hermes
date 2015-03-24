package com.ctrip.hermes.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import com.ctrip.hermes.remoting.command.Command;


public class NettyEncoder extends MessageToByteEncoder<Command> {

	@Override
	protected void encode(ChannelHandlerContext ctx, Command command, ByteBuf out) throws Exception {
		out.writeBytes(command.toByteBuffer());
	}

}
