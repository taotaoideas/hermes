package com.ctrip.hermes.core.codec;

import java.util.Map;

import org.unidal.lookup.annotation.Named;

import com.google.common.base.Charsets;

@Named(type = Codec.class, value = "cmessaging")
public class CMessagingCodec implements Codec {

	@Override
	public byte[] encode(String topic, Object obj) {
		if (obj instanceof String) {
			return ((String) obj).getBytes(Charsets.UTF_8);
		} else {
			throw new IllegalArgumentException("CMessaging producer messages should be String type, illegal message type "
			      + obj.getClass());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T decode(byte[] raw, Class<T> clazz) {
		if (clazz == byte[].class) {
			return (T) raw;
		} else {
			throw new IllegalArgumentException("CMessaging consumer messages should be byte[] type, illegal message type "
			      + clazz);
		}
	}

	@Override
	public void configure(Map<String, ?> configs) {
	}

}
