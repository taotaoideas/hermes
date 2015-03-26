package com.ctrip.hermes.codec;

import com.alibaba.fastjson.JSON;

public class JsonCodec implements Codec {

	@Override
	public byte[] encode(Object input) {
		return JSON.toJSONBytes(input);
	}

	@Override
	public <T> T decode(byte[] bytes, Class<T> clazz) {
		return JSON.parseObject(bytes, clazz);
	}

}
