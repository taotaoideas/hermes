package com.ctrip.hermes.core.message.codec;

import io.netty.buffer.ByteBuf;

import java.util.Map;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.codec.Codec;
import com.ctrip.hermes.core.codec.CodecFactory;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

@Named(type = MessageCodec.class)
public class DefaultMessageCodec implements MessageCodec {
	private Codec m_codec;

	private String m_topic;

	public DefaultMessageCodec(String topic) {
		m_codec = CodecFactory.getCodec(topic);
		m_topic = topic;
	}

	public DefaultMessageCodec(String topic, Codec codec) {
		m_codec = codec;
		m_topic = topic;
	}

	public DefaultMessageCodec() {

	}

	@Override
	public void encode(ProducerMessage<?> msg, ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		int indexBeginning = buf.writerIndex();

		// placeholder for length
		codec.writeInt(-1);

		codec.writeString(msg.getKey());
		codec.writeLong(msg.getBornTime());
		// remaining retries
		codec.writeInt(0);

		PropertiesHolder propertiesHolder = msg.getPropertiesHolder();
		writeProperties(propertiesHolder.getDurableProperties(), buf, codec);
		writeProperties(propertiesHolder.getVolatileProperties(), buf, codec);

		// TODO pass buf to m_codec
		byte[] body = m_codec.encode(msg.getTopic(), msg.getBody());
		int indexBeforeLen = buf.writerIndex();
		codec.writeInt(-1);
		int indexBeforeBody = buf.writerIndex();
		buf.writeBytes(body);
		int indexAfterBody = buf.writerIndex();
		int len = indexAfterBody - indexBeforeBody;
		buf.writerIndex(indexBeforeLen);
		codec.writeInt(len);

		buf.writerIndex(indexBeginning);
		codec.writeInt(indexAfterBody - indexBeginning);

		buf.writerIndex(indexAfterBody);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public BaseConsumerMessage<?> decode(ByteBuf buf, Class<?> bodyClazz) {
		BaseConsumerMessage msg = new BaseConsumerMessage();

		PartialDecodedMessage decodedMessage = partialDecode(buf);
		msg.setKey(decodedMessage.getKey());
		msg.setBornTime(decodedMessage.getBornTime());
		msg.setRemainingRetries(decodedMessage.getRemainingRetries());
		Map<String, String> durableProperties = readProperties(decodedMessage.getDurableProperties());
		Map<String, String> volatileProperties = readProperties(decodedMessage.getVolatileProperties());
		msg.setPropertiesHolder(new PropertiesHolder(durableProperties, volatileProperties));
		msg.setBody(m_codec.decode(decodedMessage.readBody(), bodyClazz));
		msg.setTopic(m_topic);

		return msg;
	}

	@Override
	public PartialDecodedMessage partialDecode(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		codec.readInt();
		PartialDecodedMessage msg = new PartialDecodedMessage();
		msg.setKey(codec.readString());
		msg.setBornTime(codec.readLong());
		msg.setRemainingRetries(codec.readInt());

		int len = codec.readInt();
		msg.setDurableProperties(buf.readSlice(len));

		len = codec.readInt();
		msg.setVolatileProperties(buf.readSlice(len));

		len = codec.readInt();
		msg.setBody(buf.readSlice(len));

		return msg;
	}

	@Override
	public void encode(PartialDecodedMessage msg, ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		int indexBeginning = buf.writerIndex();

		// placeholder for length
		codec.writeInt(-1);

		codec.writeString(msg.getKey());
		codec.writeLong(msg.getBornTime());
		codec.writeInt(msg.getRemainingRetries());

		writeProperties(msg.getDurableProperties(), buf, codec);
		writeProperties(msg.getVolatileProperties(), buf, codec);

		// TODO pass buf to m_codec
		ByteBuf body = msg.getBody();
		codec.writeInt(body.readableBytes());
		buf.writeBytes(body);

		int indexAfterBody = buf.writerIndex();

		buf.writerIndex(indexBeginning);
		codec.writeInt(indexAfterBody - indexBeginning);

		buf.writerIndex(indexAfterBody);
	}

	private void writeProperties(ByteBuf propertiesBuf, ByteBuf out, HermesPrimitiveCodec codec) {
		int writeIndexBeforeLength = out.writerIndex();
		codec.writeInt(-1);
		int writeIndexBeforeMap = out.writerIndex();
		if (propertiesBuf != null) {
			out.writeBytes(propertiesBuf);
		} else {
			codec.writeNull();
		}
		int mapLength = out.writerIndex() - writeIndexBeforeMap;
		int writeIndexEnd = out.writerIndex();
		out.writerIndex(writeIndexBeforeLength);
		codec.writeInt(mapLength);
		out.writerIndex(writeIndexEnd);
	}

	private void writeProperties(Map<String, String> properties, ByteBuf buf, HermesPrimitiveCodec codec) {
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

	@SuppressWarnings("unchecked")
	private Map<String, String> readProperties(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		return codec.readMap();
	}

}
