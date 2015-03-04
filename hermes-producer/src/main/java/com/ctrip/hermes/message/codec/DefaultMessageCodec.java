package com.ctrip.hermes.message.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;

public class DefaultMessageCodec implements MessageCodec {

	@Inject
	private CodecManager m_codecManager;

	@Override
	public byte[] encode(Message<?> msg) {
		Codec codec = m_codecManager.getCodec(msg.getTopic());
		byte[] msgBody = codec.encode(msg.getBody());

		ByteArrayOutputStream bout = new ByteArrayOutputStream(sizeOf(msgBody, msg));

		try {
			writeString(bout, msg.getTopic());
			writeString(bout, msg.getKey());
			writeString(bout, msg.getPartition());
			writeBoolean(bout, msg.isPriority());

			writeBytes(bout, msgBody);
		} catch (IOException e) {
			// ByteArrayOutputStream won't throw IOException
			throw new RuntimeException("Unexpected exception when write to ByteArrayOutputStream", e);
		}

		return bout.toByteArray();
	}

	private void writeBoolean(ByteArrayOutputStream out, boolean b) throws IOException {
		out.write(b ? 1 : 0);
	}

	private void writeBytes(ByteArrayOutputStream out, byte[] bytes) throws IOException {
		if (bytes == null || bytes.length == 0) {
			writeInt(out, 0);
		} else {
			writeInt(out, bytes.length);
			out.write(bytes);
		}
	}

	private int sizeOf(byte[] body, Message<?> msg) {
		// TODO
		return body.length + 100;
	}

	private void writeString(ByteArrayOutputStream out, String str) throws IOException {
		if (Strings.isNullOrEmpty(str)) {
			writeInt(out, 0);
		} else {
			byte[] bytes = str.getBytes(Charsets.UTF_8);
			writeInt(out, bytes.length);
			out.write(bytes);
		}
	}

	private void writeInt(ByteArrayOutputStream out, int i) {
		out.write((i >>> 24) & 0xFF);
		out.write((i >>> 16) & 0xFF);
		out.write((i >>> 8) & 0xFF);
		out.write((i >>> 0) & 0xFF);
	}

	@Override
	public Message<byte[]> decode(byte[] bytes) {
		Message<byte[]> msg = new Message<>();

		int cursor = 0;

		int topicLen = readInt(bytes, cursor);
		cursor += 4;

		msg.setTopic(new String(bytes, cursor, topicLen));
		cursor += topicLen;

		int keyLen = readInt(bytes, cursor);
		cursor += 4;

		msg.setKey(new String(bytes, cursor, keyLen));
		cursor += keyLen;

		int partitionLen = readInt(bytes, cursor);
		cursor += 4;

		msg.setPartition(new String(bytes, cursor, partitionLen));
		cursor += partitionLen;

		msg.setPriority(readBoolean(bytes, cursor));
		cursor += 1;

		int bodyLen = readInt(bytes, cursor);
		cursor += 4;

		if (bodyLen > 0) {
			// TODO should use ByteBuffer
			byte[] bodyBytes = new byte[bodyLen];
			System.arraycopy(bytes, cursor, bodyBytes, 0, bodyLen);
			msg.setBody(bodyBytes);
		}

		return msg;
	}

	private boolean readBoolean(byte[] bytes, int cursor) {
		return bytes[cursor] == 1;
	}

	private int readInt(byte[] bytes, int start) {
		return ((bytes[start] << 24) + (bytes[start + 1] << 16) + (bytes[start + 2] << 8) + (bytes[start + 3] << 0));
	}

}
