package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SubscribeAckCommand extends AbstractCommand implements Ack {
	private boolean m_success = true;

	public SubscribeAckCommand() {
		super(CommandType.ACK_SUBSCRIBE);
	}

	public boolean isSuccess() {
		return m_success;
	}

	public void setSuccess(boolean success) {
		m_success = success;
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_success = codec.readBoolean();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeBoolean(m_success);
	}

}
