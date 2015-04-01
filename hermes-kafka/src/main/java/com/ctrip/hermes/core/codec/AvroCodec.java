package com.ctrip.hermes.core.codec;

import java.util.Map;

import org.unidal.lookup.annotation.Named;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

@Named(type = Codec.class, value = com.ctrip.hermes.meta.entity.Codec.AVRO)
public class AvroCodec implements Codec {

	public final java.lang.String SCHEMA_REGISTRY_URL = "schema.registry.url";

	public final java.lang.String MAX_SCHEMAS_PER_SUBJECT = "max.schemas.per.subject";

	public final int DEFAULT_MAX_SCHEMAS_PER_SUBJECT = 1000;

	private SchemaRegistryClient schemaRegistry;

	private KafkaAvroSerializer avroSerializer;

	private KafkaAvroDecoder avroDeserializer;

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
		Object url = configs.get(SCHEMA_REGISTRY_URL);
		Object maxSchemaObject = configs.get(MAX_SCHEMAS_PER_SUBJECT);
		if (maxSchemaObject == null) {
			schemaRegistry = new CachedSchemaRegistryClient((String) url, DEFAULT_MAX_SCHEMAS_PER_SUBJECT);
		} else {
			schemaRegistry = new CachedSchemaRegistryClient((String) url, (Integer) maxSchemaObject);
		}

		avroSerializer = new KafkaAvroSerializer(schemaRegistry);
		avroDeserializer = new KafkaAvroDecoder(schemaRegistry);
	}

}
