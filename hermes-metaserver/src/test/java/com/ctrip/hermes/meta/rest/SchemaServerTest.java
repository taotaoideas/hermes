package com.ctrip.hermes.meta.rest;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
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
	public void testListSchemas() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
		Builder request = webTarget.path("schemas").request();
		Response response = request.get();
		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatusInfo().getStatusCode());
		List<SchemaView> schemas = response.readEntity(new GenericType<List<SchemaView>>() {
		});
		System.out.println(schemas);
		Assert.assertTrue(schemas.size() > 0);

		request = webTarget.path("schemas/" + schemas.get(0).getId()).request();
		SchemaView actual = request.get(SchemaView.class);
		System.out.println(actual);
		Assert.assertEquals(schemas.get(0).getId(), actual.getId());
	}

	@Test
	public void testPostNewJsonSchema() throws IOException {
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
		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
		form.field("schema", JSON.toJSONString(schemaView));
		Response response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
		System.out.println(response.getStatus());
	}

	@Test
	public void testPostExistingJsonSchema() throws IOException {
		String jsonString = Files.toString(new File("src/test/resources/schema-json-sample.json"), Charsets.UTF_8);
		SchemaView schemaView = JSON.parseObject(jsonString, SchemaView.class);

		ResourceConfig rc = new ResourceConfig();
		rc.register(MultiPartFeature.class);
		Client client = ClientBuilder.newClient(rc);
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
		Builder request = webTarget.path("schemas/").request();

		FormDataMultiPart form = new FormDataMultiPart();
		File file = new File("src/test/resources/schema-json-sample.json");
		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
		form.field("schema", JSON.toJSONString(schemaView));
		Response response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
		Assert.assertEquals(Status.CONFLICT.getStatusCode(), response.getStatus());
		System.out.println(response.readEntity(String.class));
	}

	@Test
	public void testUploadJsonFile() throws IOException {
		ResourceConfig rc = new ResourceConfig();
		rc.register(MultiPartFeature.class);
		Client client = ClientBuilder.newClient(rc);
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);

		Builder request = webTarget.path("schemas").request();
		Response response = request.get();
		List<SchemaView> schemas = response.readEntity(new GenericType<List<SchemaView>>() {
		});

		SchemaView jsonSchema = null;
		for (SchemaView schema : schemas) {
			if (schema.getType().equals("json")) {
				jsonSchema = schema;
				break;
			}
		}

		if (jsonSchema != null) {
			FormDataMultiPart form = new FormDataMultiPart();
			File file = new File("src/test/resources/schema-json-sample.json");
			form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
			request = webTarget.path("schemas/" + jsonSchema.getId() + "/upload").request();
			response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
			Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatusInfo().getStatusCode());

			SchemaView updatedJsonSchema = response.readEntity(SchemaView.class);
			Assert.assertEquals(jsonSchema.getVersion().intValue() + 1, updatedJsonSchema.getVersion().intValue());
		}
	}

	@Test
	public void testUploadAvroFile() {
		ResourceConfig rc = new ResourceConfig();
		rc.register(MultiPartFeature.class);
		Client client = ClientBuilder.newClient(rc);
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);

		Builder request = webTarget.path("schemas").request();
		Response response = request.get();
		List<SchemaView> schemas = response.readEntity(new GenericType<List<SchemaView>>() {
		});

		SchemaView jsonSchema = null;
		for (SchemaView schema : schemas) {
			if (schema.getType().equals("avro")) {
				jsonSchema = schema;
				break;
			}
		}

		if (jsonSchema != null) {
			FormDataMultiPart form = new FormDataMultiPart();
			File file = new File("src/test/resources/schema-avro-sample.avsc");
			form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
			request = webTarget.path("schemas/" + jsonSchema.getId() + "/upload").request();
			response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
			Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatusInfo().getStatusCode());

			SchemaView updatedJsonSchema = response.readEntity(SchemaView.class);
			Assert.assertEquals(jsonSchema.getVersion().intValue() + 1, updatedJsonSchema.getVersion().intValue());
		}
	}

	@Test
	public void testDownloadFiles() {
		ResourceConfig rc = new ResourceConfig();
		rc.register(MultiPartFeature.class);
		Client client = ClientBuilder.newClient(rc);
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);

		Builder request = webTarget.path("schemas").request();
		Response response = request.get();
		List<SchemaView> schemas = response.readEntity(new GenericType<List<SchemaView>>() {
		});

		request = webTarget.path("schemas/" + schemas.get(0).getId() + "/schema").request();
		response = request.get();
		if (response.getStatusInfo().getStatusCode() != Status.NOT_FOUND.getStatusCode()) {
			Assert.assertEquals(Status.OK.getStatusCode(), response.getStatusInfo().getStatusCode());
			File schemaFile = response.readEntity(File.class);
			System.out.println(schemaFile.getAbsolutePath());
			Assert.assertTrue(schemaFile.length() > 0);
			schemaFile.delete();
		}

		request = webTarget.path("schemas/" + schemas.get(0).getId() + "/jar").request();
		response = request.get();
		if (response.getStatusInfo().getStatusCode() != Status.NOT_FOUND.getStatusCode()) {
			Assert.assertEquals(Status.OK.getStatusCode(), response.getStatusInfo().getStatusCode());
			File jarFile = response.readEntity(File.class);
			System.out.println(jarFile.getAbsolutePath());
			Assert.assertTrue(jarFile.length() > 0);
			jarFile.delete();
		}
	}
}
