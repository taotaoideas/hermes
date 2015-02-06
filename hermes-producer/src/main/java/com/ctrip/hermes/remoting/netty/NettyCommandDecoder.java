package com.ctrip.hermes.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandCodec;

public class NettyCommandDecoder extends ByteToMessageDecoder {

	@Inject
	private CommandCodec m_codec;

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		try {
			byte[] buf = new byte[in.readableBytes()];
			in.readBytes(buf);

			Command cmd = m_codec.decode(buf);
			out.add(cmd);
		} catch (Exception e) {
			// TODO close channel
		} finally {
		}

	}

}
