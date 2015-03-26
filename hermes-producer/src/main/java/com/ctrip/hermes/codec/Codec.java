package com.ctrip.hermes.codec;

public interface Codec {
	public byte[] encode(Object obj);

	public <T> T decode(byte[] raw, Class<T> clazz);
}
