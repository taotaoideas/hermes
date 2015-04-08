package com.ctrip.hermes.meta;

import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.meta.pojo.TopicView;
import com.ctrip.hermes.meta.server.MetaRestServer;

public class TopicServerTest extends ComponentTestCase {

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
	public void testGetTopic() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target("http://0.0.0.0:8080/");
		String topic = "kafka.AvroTopic";
		Builder request = webTarget.path("topics/" + topic).request();
		String actual = request.get(String.class);
		Assert.assertNotNull(actual);
		System.out.println(actual);
	}

	@Test
	public void testListTopic() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target("http://0.0.0.0:8080/");
		Builder request = webTarget.path("topics/").queryParam("pattern", ".*").request();
		String actual = request.get(String.class);
		Assert.assertNotNull(actual);
		System.out.println(actual);
	}
}
