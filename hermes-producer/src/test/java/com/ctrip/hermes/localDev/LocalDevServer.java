package com.ctrip.hermes.localDev;

import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.servlet.GzipFilter;
import org.unidal.test.jetty.JettyServer;


public class LocalDevServer extends JettyServer {

    static LocalDevServer server = new LocalDevServer();

    public static LocalDevServer getInstance(){
        return server;
    }

    public static void main(String[] args) throws Exception {

        LocalDevServer server = LocalDevServer.getInstance();

        server.startServer();
        server.startWebapp();
        server.stopServer();
    }

    @Before
    public void before() throws Exception {
        System.setProperty("devMode", "true");
        super.startServer();
    }

    @Override
    protected String getContextPath() {
        return "/";
    }

    @Override
    protected int getServerPort() {
        return 2765;
    }

    @Override
    protected void postConfigure(WebAppContext context) {
        context.addFilter(GzipFilter.class, "/console/*", Handler.ALL);
    }

    @Test
    public void startWebapp() throws Exception {
        // open the page in the default browser
        display("/index.html");
        waitForAnyKey();
    }
}