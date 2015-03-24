package com.ctrip.hermes.remoting.command;

import io.netty.buffer.ByteBuf;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface CommandParser {

	/**
	 * @param frame
	 * @return
	 */
	Object parse(ByteBuf frame);

}
