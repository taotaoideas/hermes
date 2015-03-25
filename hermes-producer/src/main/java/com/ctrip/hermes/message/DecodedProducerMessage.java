package com.ctrip.hermes.message;

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
		byte[] bodyRawData = new byte[m_body.readableBytes()];
		getBody().readBytes(bodyRawData);
		return bodyRawData;
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

}