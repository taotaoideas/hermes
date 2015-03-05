package com.ctrip.hermes.message.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;

public class DefaultMessageCodec implements MessageCodec {

	@Inject
	private CodecManager m_codecManager;

	@Override
	public byte[] encode(Message<?> msg) {
		Codec bodyCodec = m_codecManager.getCodec(msg.getTopic());
		byte[] msgBody = bodyCodec.encode(msg.getBody());

		ByteArrayOutputStream bout = new ByteArrayOutputStream(sizeOf(msgBody, msg));
		HermesCodec codec = new HermesCodec(bout);

		try {
			codec.writeString(msg.getTopic());
			codec.writeString(msg.getKey());
			codec.writeString(msg.getPartition());
			codec.writeBoolean(msg.isPriority());
			codec.writeLong(msg.getBornTime());
			
//			codec.writeLong(msg.getProperties().size());
//			for (Map.Entry<String, Object> entry : msg.getProperties().entrySet()) {
//				
//         }
			
			codec.writeBytes(msgBody);
		} catch (IOException e) {
			// ByteArrayOutputStream won't throw IOException
			throw new RuntimeException("Unexpected exception when write to ByteArrayOutputStream", e);
		}

		return bout.toByteArray();
	}

	private int sizeOf(byte[] body, Message<?> msg) {
		// TODO
		return body.length + 100;
	}

	@Override
	public Message<byte[]> decode(byte[] bytes) {
		HermesCodec codec = new HermesCodec(bytes);
		Message<byte[]> msg = new Message<>();

		msg.setTopic(codec.readString());
		msg.setKey(codec.readString());
		msg.setPartition(codec.readString());
		msg.setPriority(codec.readBoolean());
		msg.setBornTime(codec.readLong());
		// TODO should use ByteBuffer
		msg.setBody(codec.readBytes());

		return msg;
	}

}
