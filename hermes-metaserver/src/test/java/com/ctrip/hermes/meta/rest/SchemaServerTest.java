package com.ctrip.hermes.meta.rest;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

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
		String schema = "sample_json";
		Builder request = webTarget.path("schemas/" + schema).request();
		SchemaView actual = request.get(SchemaView.class);
		System.out.println(actual);
		Assert.assertEquals(schema, actual.getName());
	}

	@Test
	public void testPostJsonSchema() throws IOException {
		String jsonString = Files.toString(new File("src/test/resources/schema-json-sample.json"), Charsets.UTF_8);
		SchemaView schemaView = JSON.parseObject(jsonString, SchemaView.class);
		schemaView.setName(schemaView.getName() + "_" + UUID.randomUUID());

		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
		Builder request = webTarget.path("schemas/").request();
		Response response = request.post(Entity.json(schemaView));
		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
		System.out.println(response.readEntity(SchemaView.class));
	}

	@Test
	public void testPutJsonSchema() throws IOException {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);

		String schema = "sample_json";
		Builder request = webTarget.path("schemas/" + schema).request();
		SchemaView actual = request.get(SchemaView.class);
		if (actual.getType().equals("json")) {
			actual.setType("avro");
		} else {
			actual.setType("json");
		}

		request = webTarget.path("schemas/" + schema).request();
		Response response = request.put(Entity.json(actual));
		Assert.assertNotNull(response);
		if (response.getStatusInfo().getStatusCode() == Status.OK.getStatusCode()) {
			System.out.println(response.readEntity(SchemaView.class));
		}
	}

}
