package com.ctrip.hermes.meta.rest;

import java.io.File;
import java.io.IOException;

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
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.server.MetaRestServer;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class MetaServerTest extends ComponentTestCase {

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
	public void testGetMeta() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
		Builder request = webTarget.path("meta/").request();
		Meta actual = request.get(Meta.class);
		System.out.println(actual);
		Assert.assertTrue(actual.getTopics().size() > 0);
	}

	@Test
	public void testPostMetaWithEntity() throws IOException {
		String jsonString = Files.toString(new File("src/test/resources/meta-sample.json"), Charsets.UTF_8);
		Meta meta = JSON.parseObject(jsonString, Meta.class);

		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
		Builder request = webTarget.path("meta/").request();
		Response response = request.post(Entity.json(meta));
		if (response.getStatus() != Status.NOT_MODIFIED.getStatusCode()) {
			System.out.println(response.readEntity(Meta.class));
		}
	}

	@Test
	public void testPostMetaWithText() throws IOException {
		String jsonString = Files.toString(new File("src/test/resources/meta-sample.json"), Charsets.UTF_8);

		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
		Builder request = webTarget.path("meta/").request();
		Response response = request.post(Entity.text(jsonString));
		if (response.getStatus() != Status.NOT_MODIFIED.getStatusCode()) {
			System.out.println(response.readEntity(Meta.class));
		}
	}
}
