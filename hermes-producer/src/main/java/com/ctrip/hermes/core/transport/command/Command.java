package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface Command {
	public Header getHeader();

	public void parse(ByteBuf buf, Header header);

	public void toBytes(ByteBuf buf);

}
