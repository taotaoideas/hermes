package com.ctrip.hermes.rest;

import java.net.URI;
import java.rmi.ServerException;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;

import com.ctrip.framework.clogging.agent.config.LogConfig;
import com.ctrip.framework.clogging.agent.log.ILog;
import com.ctrip.framework.clogging.agent.log.LogManager;
import com.ctrip.hermes.rest.common.Configuration;
import com.ctrip.hermes.rest.filter.CORSFilter;
import com.google.common.base.Preconditions;
import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;

public class HermesRestServer {

	private static ILog logger = LogManager.getLogger(HermesRestServer.class);

	private int port;
	private final String basePackage;
	HttpServer httpServer;

	public HermesRestServer( String basePackage) {
		Preconditions.checkNotNull(basePackage);
		this.basePackage = basePackage;

		init();
	}


	public void doStartup() throws ServerException {
		URI uri = UriBuilder.fromUri("http://0.0.0.0/").port(port).build();
		ResourceConfig rc = new PackagesResourceConfig(basePackage,
				  "org.codehaus.jackson.jaxrs", "com.sun.jersey.api.container.filter",
				  "com.ctrip.hermes.rest.filter");
		rc.getProperties().put(
				  ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS,
				  CORSFilter.class.getName());

		try {
			httpServer = GrizzlyServerFactory.createHttpServer(uri, rc);
			logger.info("==== Started the Hermes Rest server succeed ====");
		} catch (Throwable e) {
			logger.error("Start Hermes Rest Server error: " + e.getMessage());
		}
		logger.info(String.format("==== Hermes Rest Server <%s> started.", port));
	}

	public void init() {
		Configuration.addResource("hermes-rest.properties");

		this.port = Configuration.getInt("server.port", 1357);

		LogConfig.setAppID(Configuration.get("hermes.rest.appid", "900777"));
		LogConfig.setLoggingServerIP(Configuration.get("clog.collector.ip", "collector.logging.sh.ctriptravel.com"));
		LogConfig.setLoggingServerPort(Configuration.get("clog.collector.port","63100"));
	}

	public static void main(String[] args) throws ServerException, InterruptedException {
		HermesRestServer hermesRestServer = new HermesRestServer("com.ctrip.hermes.rest.resource");
		hermesRestServer.doStartup();

		Thread.currentThread().join();
	}
}
