package com.ctrip.hermes.message.codec;

import java.nio.ByteBuffer;

import com.ctrip.hermes.message.ProducerMessage;

public interface MessageCodec {

	public ByteBuffer encode(ProducerMessage<?> msg);

	public ProducerMessage<byte[]> decode(ByteBuffer buf);

	public void write(ProducerMessage<?> msg, byte[] msgBody, HermesPrimitiveCodec codec) ;
	public ProducerMessage<byte[]> read(HermesPrimitiveCodec codec);
	public int sizeOf(byte[] body, ProducerMessage<?> msg);
}
