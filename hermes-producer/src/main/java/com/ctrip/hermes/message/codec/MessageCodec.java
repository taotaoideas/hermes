package com.ctrip.hermes.message.codec;

import com.ctrip.hermes.message.Message;

public interface MessageCodec {

	public byte[] encode(Message<?> msg);

	// TODO should return Message<ByteBuffer>
	public Message<byte[]> decode(byte[] bytes);

}
