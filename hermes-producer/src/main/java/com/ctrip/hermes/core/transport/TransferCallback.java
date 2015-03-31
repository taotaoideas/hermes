package com.ctrip.hermes.core.transport;

import io.netty.buffer.ByteBuf;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface TransferCallback {

	public void transfer(ByteBuf buf);

}
