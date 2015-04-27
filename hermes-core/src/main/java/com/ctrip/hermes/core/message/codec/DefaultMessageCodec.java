package com.ctrip.hermes.core.message.codec;

import io.netty.buffer.ByteBuf;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MessageCodec.class)
public class DefaultMessageCodec implements MessageCodec {
	private static MessageCodecVersion VERSION = MessageCodecVersion.V1;

	@Override
	public void encode(ProducerMessage<?> msg, ByteBuf buf) {
		buf.writeByte(VERSION.getVersion());

		VERSION.getHandler().encode(msg, buf);
	}

	@Override
	public byte[] encode(ProducerMessage<?> msg) {
		return VERSION.getHandler().encode(msg, VERSION.getVersion());
	}

	@Override
	public PartialDecodedMessage partialDecode(ByteBuf buf) {
		MessageCodecVersion version = getVersion(buf);
		return version.getHandler().partialDecode(buf);
	}

	@Override
	public BaseConsumerMessage<?> decode(ByteBuf buf, Class<?> bodyClazz) {
		MessageCodecVersion version = getVersion(buf);
		return version.getHandler().decode(buf, bodyClazz);
	}

	@Override
	public void encode(PartialDecodedMessage msg, ByteBuf buf) {
		buf.writeByte(VERSION.getVersion());

		VERSION.getHandler().encode(msg, buf);
	}

	private MessageCodecVersion getVersion(ByteBuf buf) {
		byte versionByte = buf.readByte();
		MessageCodecVersion version = MessageCodecVersion.valueOf(versionByte);
		if (version == null) {
			throw new IllegalArgumentException(String.format("Unknown version %d", versionByte));
		}

		return version;
	}

}
