package com.ctrip.hermes.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.ByteBuffer;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.remoting.CommandCodec;

public class NettyDecoder extends LengthFieldBasedFrameDecoder {

	@Inject
	private CommandCodec m_codec;

	public NettyDecoder() {
		super(Integer.MAX_VALUE, 0, 4, 0, 4);
	}

	@Override
	protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
		ByteBuf frame = null;
		try {
			frame = (ByteBuf) super.decode(ctx, in);
			if (null == frame) {
				return null;
			}

			ByteBuffer byteBuf = frame.nioBuffer();
			byte[] data = new byte[byteBuf.limit()];

			byteBuf.get(data);

			return m_codec.decode(data);
		} catch (Exception e) {
			// TODO close channel
			e.printStackTrace();
		} finally {
			if (null != frame) {
				frame.release();
			}
		}

		return null;
	}

}
