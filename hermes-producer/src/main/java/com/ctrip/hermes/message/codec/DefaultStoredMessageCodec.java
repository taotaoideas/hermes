package com.ctrip.hermes.message.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.storage.storage.Offset;

public class DefaultStoredMessageCodec implements StoredMessageCodec {

	@Override
	public byte[] encode(List<StoredMessage<byte[]>> msgs) {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		HermesCodec codec = new HermesCodec(bout);

		codec.writeInt(msgs.size());

		try {
			for (StoredMessage<byte[]> msg : msgs) {
				writeMsg(codec, msg);
			}
		} catch (IOException e) {
			// ByteArrayOutputStream won't throw IOException
			throw new RuntimeException("Unexpected exception when write to ByteArrayOutputStream", e);
		}

		return bout.toByteArray();
	}

	@Override
	public List<StoredMessage<byte[]>> decode(byte[] bytes) {
		HermesCodec codec = new HermesCodec(bytes);

		int listSize = codec.readInt();
		List<StoredMessage<byte[]>> result = new ArrayList<StoredMessage<byte[]>>(listSize);

		for (int i = 0; i < listSize; i++) {
			result.add(readMsg(codec));
		}

		return result;
	}

	private void writeMsg(HermesCodec codec, StoredMessage<byte[]> msg) throws IOException {
		codec.writeBytes(msg.getBody());
		writeOffset(codec, msg.getAckOffset());
		codec.writeString(msg.getKey());
		writeOffset(codec, msg.getOffset());
		codec.writeString(msg.getPartition());
		codec.writeString(msg.getTopic());
	}

	private StoredMessage<byte[]> readMsg(HermesCodec codec) {
		StoredMessage<byte[]> msg = new StoredMessage<byte[]>();

		msg.setBody(codec.readBytes());
		msg.setAckOffset(readOffset(codec));
		msg.setKey(codec.readString());
		msg.setOffset(readOffset(codec));
		msg.setPartition(codec.readString());
		msg.setTopic(codec.readString());

		return msg;
	}

	private void writeOffset(HermesCodec codec, Offset offset) throws IOException {
		if (offset == null) {
			codec.writeNull();
		} else {
			codec.writeString(offset.getId());
			codec.writeLong(offset.getOffset());
		}
	}

	private Offset readOffset(HermesCodec codec) {
		Offset offset = null;
		
		if (!codec.nextNull()) {
			offset = new Offset(codec.readString(), codec.readLong());
		}

		return offset;
	}

}
