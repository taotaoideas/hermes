package com.ctrip.hermes.meta.rest;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.pojo.SchemaView;
import com.ctrip.hermes.meta.server.MetaRestServer;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class SchemaServerTest extends ComponentTestCase {

	private MetaRestServer server;

	@Before
	public void startServer() {
		server = lookup(MetaRestServer.class);
		server.start();
	}

	@After
	public void stopServer() {
		server.stop();
	}

	@Test
	public void testGetSchema() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
		long schemaId = 21;
		Builder request = webTarget.path("schemas/" + schemaId).request();
		SchemaView actual = request.get(SchemaView.class);
		System.out.println(actual);
		Assert.assertEquals(schemaId, actual.getId());
	}

	@Test
	public void testPostJsonSchema() throws IOException {
		String jsonString = Files.toString(new File("src/test/resources/schema-json-sample.json"), Charsets.UTF_8);
		SchemaView schemaView = JSON.parseObject(jsonString, SchemaView.class);
		schemaView.setName(schemaView.getName() + "_" + UUID.randomUUID());

		ResourceConfig rc = new ResourceConfig();
		rc.register(MultiPartFeature.class);
		Client client = ClientBuilder.newClient(rc);
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
		Builder request = webTarget.path("schemas/").request();

		FormDataMultiPart form = new FormDataMultiPart();
		File file = new File("src/test/resources/schema-json-sample.json");
		form.bodyPart(new FileDataBodyPart("schema-content", file, MediaType.MULTIPART_FORM_DATA_TYPE));
		form.field("schema", JSON.toJSONString(schemaView));
		Response response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
		System.out.println(response.getStatus());
	}

	@Test
	public void testPutJsonSchema() throws IOException {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);

		long schemaId = 23;
		Builder request = webTarget.path("schemas/" + schemaId).request();
		SchemaView actual = request.get(SchemaView.class);
		if (actual.getType().equals("json")) {
			actual.setType("avro");
		} else {
			actual.setType("json");
		}

		request = webTarget.path("schemas/" + schemaId).request();
		Response response = request.put(Entity.json(actual));
		Assert.assertNotNull(response);
		if (response.getStatusInfo().getStatusCode() == Status.OK.getStatusCode()) {
			System.out.println(response.readEntity(SchemaView.class));
		}
	}

	@Test
	public void testUploadJsonFile() throws IOException {
		ResourceConfig rc = new ResourceConfig();
		rc.register(MultiPartFeature.class);
		Client client = ClientBuilder.newClient(rc);
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);

		long schemaId = 21;
		Builder request = webTarget.path("schemas/" + schemaId).request();
		SchemaView actual = request.get(SchemaView.class);
		actual.setType("json");

		FormDataMultiPart form = new FormDataMultiPart();
		File file = new File("src/test/resources/schema-json-sample.json");
		form.bodyPart(new FileDataBodyPart("schema-content", file, MediaType.MULTIPART_FORM_DATA_TYPE));
		request = webTarget.path("schemas/" + schemaId + "/upload").request();
		Response response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
		System.out.println(response.getStatus());
	}
	
	@Test
	public void testUploadAvroFile() {
		ResourceConfig rc = new ResourceConfig();
		rc.register(MultiPartFeature.class);
		Client client = ClientBuilder.newClient(rc);
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);

		long schemaId = 19;
		Builder request = webTarget.path("schemas/" + schemaId).request();
		SchemaView actual = request.get(SchemaView.class);
		actual.setType("avro");

		FormDataMultiPart form = new FormDataMultiPart();
		File file = new File("src/test/resources/schema-avro-sample.avsc");
		form.bodyPart(new FileDataBodyPart("schema-content", file, MediaType.MULTIPART_FORM_DATA_TYPE));
		request = webTarget.path("schemas/" + schemaId + "/upload").request();
		Response response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
		System.out.println(response.getStatus());
	}

	@Test
	public void testDownloadJsonSchemaFile() {
		ResourceConfig rc = new ResourceConfig();
		rc.register(MultiPartFeature.class);
		Client client = ClientBuilder.newClient(rc);
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);

		long schemaId = 23;
		Builder request = webTarget.path("schemas/" + schemaId + "/schema").request();
		Response response = request.get();
		System.out.println(response.getStatus());
		File downloadFile = response.readEntity(File.class);
		Assert.assertTrue(downloadFile.length() > 0);
	}

	@Test
	public void testDownloadAvroSchemaFile() {
		ResourceConfig rc = new ResourceConfig();
		rc.register(MultiPartFeature.class);
		Client client = ClientBuilder.newClient(rc);
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);

		long schemaId = 23;
		Builder request = webTarget.path("schemas/" + schemaId + "/schema").request();
		Response response = request.get();
		System.out.println(response.getStatus());
		File downloadFile = response.readEntity(File.class);
		Assert.assertTrue(downloadFile.length() > 0);
	}
}
