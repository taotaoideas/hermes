package com.ctrip.hermes.message.codec;

import java.nio.ByteBuffer;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;

public class DefaultMessageCodec implements MessageCodec {

	@Inject
	private CodecManager m_codecManager;

	@Override
	public ByteBuffer encode(Message<?> msg) {
		Codec bodyCodec = m_codecManager.getCodec(msg.getTopic());
		byte[] msgBody = bodyCodec.encode(msg.getBody());

		ByteBuffer buf = ByteBuffer.allocateDirect(sizeOf(msgBody, msg));

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		codec.writeString(msg.getTopic());
		codec.writeString(msg.getKey());
		codec.writeString(msg.getPartition());
		codec.writeBoolean(msg.isPriority());
		codec.writeLong(msg.getBornTime());

		codec.writeInt(msg.getProperties().size());
		for (Map.Entry<String, Object> entry : msg.getProperties().entrySet()) {
			codec.writeString(entry.getKey());
			// TODO support non-string property
			codec.writeString((String) entry.getValue());
		}

		codec.writeBytes(msgBody);

		return buf;
	}

	@Override
	public Message<byte[]> decode(ByteBuffer buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		Message<byte[]> msg = new Message<>();

		msg.setTopic(codec.readString());
		msg.setKey(codec.readString());
		msg.setPartition(codec.readString());
		msg.setPriority(codec.readBoolean());
		msg.setBornTime(codec.readLong());

		int propertiesSize = codec.readInt();
		for (int i = 0; i < propertiesSize; i++) {
			String name = codec.readString();
			String value = codec.readString();
			msg.addProperty(name, value);
		}

		msg.setBody(codec.readBytes());

		return msg;
	}

	private int sizeOf(byte[] body, Message<?> msg) {
		// TODO
		return body.length + 4 * 1000;
	}

}
