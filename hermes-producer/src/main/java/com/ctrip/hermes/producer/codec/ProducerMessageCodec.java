package com.ctrip.hermes.producer.codec;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.producer.DecodedProducerMessage;
import com.ctrip.hermes.producer.ProducerMessage;

public interface ProducerMessageCodec {

	public void encode(ProducerMessage<?> msg, ByteBuf buf);

	public DecodedProducerMessage decode(ByteBuf buf);
}
