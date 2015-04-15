package com.ctrip.hermes.meta.rest;

import java.util.Properties;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.server.MetaPropertiesLoader;
import com.ctrip.hermes.meta.server.MetaRestServer;

public class StandaloneRestServer {

	public static String HOST = null;

	static {
		Properties load = MetaPropertiesLoader.load();
		String host = load.getProperty("meta-host");
		String port = load.getProperty("meta-port");
		HOST = "http://" + host + ":" + port + "/";
	}

	public static void main(String[] args) throws InterruptedException {
		MetaRestServer server = PlexusComponentLocator.lookup(MetaRestServer.class);
		server.start();
		Thread.currentThread().join();
	}
}
