package com.ctrip.hermes.core.message.codec;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.message.DecodedProducerMessage;
import com.ctrip.hermes.core.message.ProducerMessage;

public interface ProducerMessageCodec {

	public void encode(ProducerMessage<?> msg, ByteBuf buf);

	public DecodedProducerMessage decode(ByteBuf buf);
}
