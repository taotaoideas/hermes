package com.ctrip.hermes.remoting.command;

import java.nio.ByteBuffer;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface Command {
	public Header getHeader();

	public void parse(ByteBuffer buf, Header header);

	public ByteBuffer toByteBuffer();

}
