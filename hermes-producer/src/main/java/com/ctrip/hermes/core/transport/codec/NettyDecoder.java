package com.ctrip.hermes.core.transport.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import com.ctrip.hermes.core.transport.command.parser.CommandParser;
import com.ctrip.hermes.core.transport.command.parser.DefaultCommandParser;

public class NettyDecoder extends LengthFieldBasedFrameDecoder {

	private CommandParser m_commandParser = new DefaultCommandParser();

	public NettyDecoder() {
		super(Integer.MAX_VALUE, 0, 4, 0, 4);
	}

	@Override
	protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
		ByteBuf frame = null;
		try {
			frame = (ByteBuf) super.decode(ctx, in);
			if (frame == null) {
				return null;
			}

			return m_commandParser.parse(frame);
		} catch (Exception e) {
			// TODO close channel
			e.printStackTrace();
		} finally {
			if (null != frame) {
				// TODO
//				frame.release();
			}
		}

		return null;
	}

}
