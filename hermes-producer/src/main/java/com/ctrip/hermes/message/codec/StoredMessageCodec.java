package com.ctrip.hermes.message.codec;

import java.util.List;

import com.ctrip.hermes.message.StoredMessage;

public interface StoredMessageCodec {

	public byte[] encode(List<StoredMessage<byte[]>> bytes);

	public List<StoredMessage<byte[]>> decode(byte[] bytes);

}
