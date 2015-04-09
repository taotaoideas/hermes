package com.ctrip.hermes.meta.service;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

import org.apache.avro.Schema;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.dal.meta.SchemaDao;

@Named
public class SchemaService {

	private SchemaRegistryClient avroSchemaRegistry = new CachedSchemaRegistryClient("http://10.3.8.63:8081", 1000);

	@Inject
	private SchemaDao schemaDao;

	public void registerAvro(String topic, Schema schema) throws IOException, RestClientException {
		this.avroSchemaRegistry.register(topic, schema);
	}

	public Schema getAvroSchema(int id) {
		Schema schema = null;
		try {
			schema = this.avroSchemaRegistry.getByID(id);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RestClientException e) {
			e.printStackTrace();
		}
		return schema;
	}

	public SchemaMetadata getLatestAvroSchemaMetadata(String topic) {
		SchemaMetadata schemaMetadata = null;
		try {
			schemaMetadata = this.avroSchemaRegistry.getLatestSchemaMetadata(topic);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RestClientException e) {
			e.printStackTrace();
		}
		return schemaMetadata;
	}
}
