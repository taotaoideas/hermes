package com.ctrip.hermes.message.codec;

import java.nio.ByteBuffer;
import java.util.List;

import com.ctrip.hermes.message.StoredMessage;

public interface StoredMessageCodec {

	public ByteBuffer encode(List<StoredMessage<byte[]>> bytes);

	public List<StoredMessage<byte[]>> decode(ByteBuffer buf);

}
