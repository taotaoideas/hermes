package com.ctrip.hermes.core.message;

import io.netty.buffer.ByteBuf;

public class DecodedProducerMessage {
	private ByteBuf m_body;

	private String m_key;

	private long m_bornTime;

	private ByteBuf m_appProperties;

	private ByteBuf m_sysProperties;

	public void setBody(ByteBuf body) {
		m_body = body;
	}

	public void setKey(String key) {
		m_key = key;
	}

	public void setBornTime(long bornTime) {
		m_bornTime = bornTime;
	}

	public void setAppProperties(ByteBuf appProperties) {
		m_appProperties = appProperties;
	}

	public void setSysProperties(ByteBuf sysProperties) {
		m_sysProperties = sysProperties;
	}

	public ByteBuf getBody() {
		return m_body.duplicate();
	}

	public byte[] readBody() {
		return readByteBuf(m_body);
	}

	public String getKey() {
		return m_key;
	}

	public long getBornTime() {
		return m_bornTime;
	}

	public ByteBuf getAppProperties() {
		return m_appProperties.duplicate();
	}

	public ByteBuf getSysProperties() {
		return m_sysProperties.duplicate();
	}

	public byte[] readAppProperties() {
		return readByteBuf(m_appProperties);
	}

	public byte[] readSysProperties() {
		return readByteBuf(m_sysProperties);
	}

	private byte[] readByteBuf(ByteBuf buf) {
		byte[] bytes = new byte[buf.readableBytes()];
		buf.readBytes(bytes);
		return bytes;
	}

}