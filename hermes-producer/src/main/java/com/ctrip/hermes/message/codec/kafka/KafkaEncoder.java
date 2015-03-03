package com.ctrip.hermes.message.codec.kafka;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import com.ctrip.hermes.storage.message.Message;

public class KafkaEncoder implements Encoder<Message> {

	public KafkaEncoder(VerifiableProperties prop) {

	}

	@Override
	public byte[] toBytes(Message msg) {
		//TODO Convert header properties into bytes
		return msg.getContent();
	}

}
