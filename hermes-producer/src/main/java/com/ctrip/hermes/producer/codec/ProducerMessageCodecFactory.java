package com.ctrip.hermes.producer.codec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ProducerMessageCodecFactory {

	public static ProducerMessageCodec getCodec(String topic) {
		return new DefaultProducerMessageCodec(topic);
	}

}
