package com.ctrip.hermes.meta.rest;

import java.util.Properties;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.meta.server.MetaPropertiesLoader;
import com.ctrip.hermes.meta.server.MetaRestServer;

public class StandaloneRestServer extends ComponentTestCase {

	public static String HOST = null;

	static {
		Properties load = MetaPropertiesLoader.load();
		String host = load.getProperty("meta-host");
		String port = load.getProperty("meta-port");
		HOST = "http://" + host + ":" + port + "/";
	}

	@Test
	public void startServer() throws InterruptedException {
		MetaRestServer server = lookup(MetaRestServer.class);
		server.start();
		Thread.currentThread().join();
	}
}
