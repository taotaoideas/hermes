package com.ctrip.hermes.core.message.codec;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;

public interface MessageCodec {

	public void encode(ProducerMessage<?> msg, ByteBuf buf);

	public PartialDecodedMessage partialDecode(ByteBuf buf);

	public BaseConsumerMessage<?> decode(ByteBuf buf, Class<?> bodyClazz);

	void encode(PartialDecodedMessage msg, ByteBuf buf);
}
