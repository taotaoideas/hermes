package com.ctrip.hermes.message.codec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class MessageCodecFactory {

	public static MessageCodec getMessageCodec(String topic) {
		return new DefaultMessageCodec();
	}

}
