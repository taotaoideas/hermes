package com.ctrip.hermes.remoting.command;

import java.nio.ByteBuffer;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractCommand implements Command {
	private Header m_header = new Header();

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
	public void parse(ByteBuffer buf, Header header) {
		m_header = header;
		doParse(buf);
	}

	public ByteBuffer toByteBuffer() {
		ByteBuffer headerBytes = m_header.toByteBuffer();
		headerBytes.flip();
		ByteBuffer bodyBytes = doToByteBuffer();
		bodyBytes.flip();

		ByteBuffer bytes = ByteBuffer.allocate(headerBytes.limit() + bodyBytes.limit());
		bytes.put(headerBytes).put(bodyBytes);
		bytes.flip();

		return bytes;
	}

	public abstract ByteBuffer doToByteBuffer();

	public abstract void doParse(ByteBuffer buf);

}
