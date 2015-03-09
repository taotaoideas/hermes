package com.ctrip.hermes.local;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

public class LocalDevServer {

	private static LocalDevServer instance = new LocalDevServer();

	public static LocalDevServer getInstance() {
		return instance;
	}

	private LocalDevServer() {

	}

	public void start() throws Exception {
		Server server = new Server(2765);
		WebAppContext ctx = new WebAppContext();

		ctx.setContextPath("/");
		ctx.setWar(this.getClass().getResource("/webapp").toString());
		server.setHandler(ctx);

		server.start();
	}
}