package com.ctrip.hermes.message.codec;

public interface CodecManager {

	Codec getCodec(String topic);

}
