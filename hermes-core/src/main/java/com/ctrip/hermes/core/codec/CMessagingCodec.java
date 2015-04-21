package com.ctrip.hermes.core.codec;

import java.util.Map;

import org.unidal.lookup.annotation.Named;

import com.google.common.base.Charsets;

@Named(type = Codec.class, value = com.ctrip.hermes.meta.entity.Codec.CMESSAGING)
public class CMessagingCodec implements Codec {

	@Override
	public byte[] encode(String topic, Object obj) {
		if (obj instanceof String) {
			return ((String) obj).getBytes(Charsets.ISO_8859_1);
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

	@Override
	public String getType() {
		return com.ctrip.hermes.meta.entity.Codec.CMESSAGING;
	}

}
