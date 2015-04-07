package com.ctrip.hermes.meta;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.rest.MetaRestServer;

public class CodecServiceTest extends ComponentTestCase {

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
	public void testGetCodec() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target("http://0.0.0.0:8080/");
		String topic = "kafka.SimpleTopic";
		Builder request = webTarget.path("codec/" + topic).request();
		Codec actual = request.get(Codec.class);
		Assert.assertNotNull(actual);
		System.out.println(actual);
	}
	
	@Test
	public void testGetCodecString(){
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target("http://0.0.0.0:8080/");
		String topic = "kafka.SimpleTopic";
		Builder request = webTarget.path("codec/" + topic).request();
		String actual = request.get(String.class);
		Assert.assertNotNull(actual);
		System.out.println(actual);
	}
}
