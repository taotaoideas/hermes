package com.ctrip.hermes.message.codec;

import java.nio.ByteBuffer;

import com.ctrip.hermes.message.ProducerMessage;

public interface MessageCodec {

	public ByteBuffer encode(ProducerMessage<?> msg);

	public ProducerMessage<byte[]> decode(ByteBuffer buf);
}
