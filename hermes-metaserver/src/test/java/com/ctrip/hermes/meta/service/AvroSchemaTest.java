package com.ctrip.hermes.meta.service;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.meta.avro.AvroVisitEvent;

public class AvroSchemaTest extends ComponentTestCase {

//	@Test
//	public void registerAndGetSchema() {
//		Schema expected = AvroVisitEvent.getClassSchema();
//		String topic = "test-topic";
//		SchemaService schemaService = lookup(SchemaService.class);
//		try {
//			schemaService.registerAvro(topic, expected);
//			SchemaMetadata latestAvroSchemaMetadata = schemaService.getLatestAvroSchemaMetadata(topic);
//			System.out.println(latestAvroSchemaMetadata);
//			Schema actual = schemaService.getAvroSchema(latestAvroSchemaMetadata.getId());
//			System.out.println(actual);
//			Assert.assertEquals(expected, actual);
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (RestClientException e) {
//			e.printStackTrace();
//		}
//	}

}
