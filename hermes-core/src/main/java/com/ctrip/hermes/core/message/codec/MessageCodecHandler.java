package com.ctrip.hermes.core.message.codec;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageCodecHandler {

	void encode(ProducerMessage<?> msg, ByteBuf buf);

	PartialDecodedMessage partialDecode(ByteBuf buf);

	BaseConsumerMessage<?> decode(ByteBuf buf, Class<?> bodyClazz);

	void encode(PartialDecodedMessage msg, ByteBuf buf);

	byte[] encode(ProducerMessage<?> msg, byte version);

}
