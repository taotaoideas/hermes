package com.ctrip.hermes.meta.service;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

import org.apache.avro.Schema;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.meta.avro.AvroVisitEvent;

public class AvroSchemaTest extends ComponentTestCase {

	@Test
	public void registerAndGetSchema() {
		Schema schema = AvroVisitEvent.getClassSchema();
		String topic = "test-topic";
		SchemaService schemaService = lookup(SchemaService.class);
		try {
			schemaService.registerAvro(topic, schema);
			Schema avroSchema = schemaService.getAvroSchema(topic);
			System.out.println(avroSchema);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RestClientException e) {
			e.printStackTrace();
		}
	}

}
