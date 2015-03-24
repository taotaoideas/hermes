package com.ctrip.hermes.message.codec;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.message.ProducerMessage;

public interface MessageCodec {

	public void encode(ProducerMessage<?> msg, ByteBuf buf);

	public ProducerMessage<?> decode(ByteBuf buf);
}
