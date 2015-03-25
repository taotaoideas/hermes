package com.ctrip.hermes.message.codec;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.message.DecodedProducerMessage;
import com.ctrip.hermes.message.ProducerMessage;

public interface MessageCodec {

	public void encode(ProducerMessage<?> msg, ByteBuf buf);

	public DecodedProducerMessage decode(ByteBuf buf);
}
