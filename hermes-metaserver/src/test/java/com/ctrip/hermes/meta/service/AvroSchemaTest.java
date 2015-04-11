package com.ctrip.hermes.meta.service;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.meta.avro.AvroVisitEvent;

public class AvroSchemaTest extends ComponentTestCase {

	@Test
	public void registerAndGetSchema() {
		Schema expected = AvroVisitEvent.getClassSchema();
		String topic = "test-topic";
		SchemaService schemaService = lookup(SchemaService.class);
		try {
			schemaService.createAvroSchema(topic, expected);
			org.apache.avro.Schema avroSchema = schemaService.getAvroSchema(topic);
			System.out.println(avroSchema);
			Assert.assertEquals(expected, avroSchema);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RestClientException e) {
			e.printStackTrace();
		} catch (DalException e) {
			e.printStackTrace();
		}
	}

}
