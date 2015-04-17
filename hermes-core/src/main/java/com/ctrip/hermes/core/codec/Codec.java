package com.ctrip.hermes.core.codec;

import java.util.Map;

public interface Codec {

	public String getType();

	public byte[] encode(String topic, Object obj);

	public <T> T decode(byte[] raw, Class<T> clazz);

	public void configure(Map<String, ?> configs);
}
