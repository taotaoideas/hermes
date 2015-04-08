package com.ctrip.hermes.kafka.codec;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import kafka.utils.VerifiableProperties;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.codec.Codec;

@Named(type = Codec.class, value = com.ctrip.hermes.meta.entity.Codec.AVRO)
public class AvroCodec implements Codec {

	private KafkaAvroSerializer avroSerializer;

	private KafkaAvroDecoder avroDeserializer;

	@SuppressWarnings("unchecked")
	@Override
	public <T> T decode(byte[] raw, Class<T> clazz) {
		return (T) avroDeserializer.fromBytes(raw);
	}

	@Override
	public byte[] encode(String topic, Object obj) {
		return avroSerializer.serialize(topic, obj);
	}

	@Override
	public void configure(Map<String, ?> configs) {
		avroSerializer = new KafkaAvroSerializer();
		avroSerializer.configure(configs, false);
		Properties prop = new Properties();
		for (Entry<String, ?> entry : configs.entrySet()) {
			prop.setProperty(entry.getKey(), entry.getValue().toString());
		}
		prop.setProperty("specific.avro.reader", Boolean.TRUE.toString());
		avroDeserializer = new KafkaAvroDecoder(new VerifiableProperties(prop));
	}
}
