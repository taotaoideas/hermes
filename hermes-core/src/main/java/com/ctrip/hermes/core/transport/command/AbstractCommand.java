package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractCommand implements Command {
	private static final long serialVersionUID = 1160178108416493829L;

	protected Header m_header = new Header();

	protected ByteBuf m_rawBuf;

	public AbstractCommand(CommandType commandType) {
		m_header.setType(commandType);
	}

	public Header getHeader() {
		return m_header;
	}

	public void setHeader(Header header) {
		m_header = header;
	}

	public static Command valueOf(ByteBuffer data) {
		return new SendMessageCommand();
	}

	@Override
	public void parse(ByteBuf buf, Header header) {
		m_header = header;
		m_rawBuf = buf;
		parse0(buf);
	}

	public void release() {
		if (m_rawBuf != null) {
			m_rawBuf.release();
		}
	}

	public void toBytes(ByteBuf buf) {
		m_header.toBytes(buf);
		toBytes0(buf);
	}

	public void correlate(Command req) {
		m_header.setCorrelationId(req.getHeader().getCorrelationId());
	}

	protected abstract void toBytes0(ByteBuf buf);

	protected abstract void parse0(ByteBuf buf);

}
