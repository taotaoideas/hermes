package com.ctrip.hermes.core.message.codec;

import io.netty.buffer.ByteBuf;

import java.util.Map;

import com.ctrip.hermes.core.codec.Codec;
import com.ctrip.hermes.core.codec.CodecFactory;
import com.ctrip.hermes.core.message.DecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

public class DefaultMessageCodec implements MessageCodec {
	private Codec m_codec ;

   public DefaultMessageCodec(String topic) {
	  m_codec = CodecFactory.getCodec(topic);
   }

	@Override
	public void encode(ProducerMessage<?> msg, ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		codec.writeString(msg.getKey());
		codec.writeLong(msg.getBornTime());

		writeProperties(msg.getAppProperties(), buf, codec);
		writeProperties(msg.getSysProperties(), buf, codec);

		// TODO pass buf to m_codec
		byte[] body = m_codec.encode(msg.getBody());
		int indexBeforeLen = buf.writerIndex();
		codec.writeInt(-1);
		int indexBeforeBody = buf.writerIndex();
		buf.writeBytes(body);
		int indexAfterBody = buf.writerIndex();
		int len = indexAfterBody - indexBeforeBody;
		buf.writerIndex(indexBeforeLen);
		codec.writeInt(len);
		buf.writerIndex(indexAfterBody);
	}

	private void writeProperties(Map<String, Object> properties, ByteBuf buf, HermesPrimitiveCodec codec) {
		int writeIndexBeforeLength = buf.writerIndex();
		codec.writeInt(-1);
		int writeIndexBeforeMap = buf.writerIndex();
		codec.writeMap(properties);
		int mapLength = buf.writerIndex() - writeIndexBeforeMap;
		int writeIndexEnd = buf.writerIndex();
		buf.writerIndex(writeIndexBeforeLength);
		codec.writeInt(mapLength);
		buf.writerIndex(writeIndexEnd);
	}

	@Override
	public DecodedMessage decode(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		DecodedMessage msg = new DecodedMessage();
		msg.setKey(codec.readString());
		msg.setBornTime(codec.readLong());

		int len = codec.readInt();
		msg.setAppProperties(buf.readSlice(len));

		len = codec.readInt();
		msg.setSysProperties(buf.readSlice(len));

		len = codec.readInt();
		msg.setBody(buf.readSlice(len));

		return msg;
	}
}
