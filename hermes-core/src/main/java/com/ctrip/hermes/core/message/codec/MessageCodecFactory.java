package com.ctrip.hermes.core.message.codec;


/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class MessageCodecFactory {

	public static MessageCodec getCodec(String topic) {
		return new DefaultMessageCodec(topic);
	}

}
