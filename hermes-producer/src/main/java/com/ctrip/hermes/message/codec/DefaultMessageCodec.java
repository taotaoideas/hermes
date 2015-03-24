package com.ctrip.hermes.message.codec;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.message.ProducerMessage;
import com.ctrip.hermes.message.codec.internal.JsonCodec;

public class DefaultMessageCodec implements MessageCodec {
	// TODO
	private Codec m_codec = new JsonCodec();

	@Override
	public void encode(ProducerMessage<?> msg, ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		codec.writeBytes(m_codec.encode(msg));

	}

	@Override
	public ProducerMessage<?> decode(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		byte[] data = codec.readBytes();

		return m_codec.decode(data, ProducerMessage.class);
	}
}
