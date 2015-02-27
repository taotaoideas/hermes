package com.ctrip.hermes.message.codec.kafka;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

import com.ctrip.hermes.storage.message.Message;

public class KafkaDecoder implements Decoder<Message> {
	
	public KafkaDecoder(VerifiableProperties prop) {

	}

	@Override
	public Message fromBytes(byte[] bytes) {
		Message msg = new Message();
		msg.setContent(bytes);
		return msg;
	}

}
