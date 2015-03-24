package com.ctrip.hermes.message.codec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class MessageCodecFactory {

	/**
	 * @param first
	 * @return
	 */
   public static MessageCodec getMessageCodec(String first) {
	   return new DefaultMessageCodec();
   }

}
