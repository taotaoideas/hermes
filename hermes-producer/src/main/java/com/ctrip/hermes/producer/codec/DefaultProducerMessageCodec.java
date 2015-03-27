package com.ctrip.hermes.producer.codec;

import io.netty.buffer.ByteBuf;

import java.util.Map;

import com.ctrip.hermes.core.codec.Codec;
import com.ctrip.hermes.core.codec.CodecFactory;
import com.ctrip.hermes.core.message.DecodedProducerMessage;
import com.ctrip.hermes.producer.ProducerMessage;
import com.ctrip.hermes.utils.HermesPrimitiveCodec;

public class DefaultProducerMessageCodec implements ProducerMessageCodec {
	private Codec m_codec ;

   public DefaultProducerMessageCodec(String topic) {
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
	public DecodedProducerMessage decode(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		DecodedProducerMessage msg = new DecodedProducerMessage();
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
