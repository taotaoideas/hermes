package com.ctrip.hermes.meta.service;

import java.io.IOException;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.avro.Schema;
import org.unidal.lookup.annotation.Named;

@Named
public class SchemaService {

	private SchemaRegistryClient avroSchemaRegistry = new CachedSchemaRegistryClient("http://10.3.8.63:8081", 1000);

	public void registerAvro(String topic, Schema schema) throws IOException, RestClientException {
		this.avroSchemaRegistry.register(topic, schema);
	}

	public Schema getAvroSchema(String topic) throws IOException, RestClientException {
		SchemaMetadata latestSchemaMetadata = this.avroSchemaRegistry.getLatestSchemaMetadata(topic);
		Schema schema = this.avroSchemaRegistry.getByID(latestSchemaMetadata.getId());
		return schema;
	}
}
