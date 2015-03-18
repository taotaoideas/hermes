package com.ctrip.hermes.message.codec;

import java.nio.ByteBuffer;

import com.ctrip.hermes.message.Message;

public interface MessageCodec {

	public ByteBuffer encode(Message<?> msg);

	public Message<byte[]> decode(ByteBuffer buf);
}
