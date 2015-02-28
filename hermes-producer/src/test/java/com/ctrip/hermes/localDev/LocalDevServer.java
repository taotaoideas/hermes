package com.ctrip.hermes.localDev;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.servlet.GzipFilter;
import org.unidal.lookup.annotation.Inject;
import org.unidal.test.jetty.JettyServer;

import com.ctrip.hermes.channel.MessageQueueMonitor;
import com.ctrip.hermes.producer.Producer;


public class LocalDevServer extends JettyServer {

    @Inject
    private static MessageQueueMonitor monitor;

    public static void main(String[] args) throws Exception {
        LocalDevServer server = new LocalDevServer();

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

        sendMsg("order.new", "hello");

        Thread.sleep(1000);
        // open the page in the default browser
        display("/api/meta");
        waitForAnyKey();
    }

    public static MessageQueueMonitor.MessageQueueStatus getQueueStatus() throws Exception {
        return monitor.status();
    }

    public void sendMsg(String topic, String msg) {
        String uuid = UUID.randomUUID().toString();
        Producer.getInstance().message(topic, msg).withKey(uuid).send();
    }
}