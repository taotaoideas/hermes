package com.ctrip.hermes.localDev;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.servlet.GzipFilter;
import org.unidal.test.jetty.JettyServer;

import com.ctrip.hermes.channel.ConsumerChannel;
import com.ctrip.hermes.channel.ConsumerChannelHandler;
import com.ctrip.hermes.channel.LocalMessageChannelManager;
import com.ctrip.hermes.channel.MessageChannelManager;
import com.ctrip.hermes.message.StoredMessage;


public class LocalDevServer extends JettyServer {

    static LocalDevServer server = new LocalDevServer();

    public static LocalDevServer getInstance() {
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

        startConsumerGroup("order.new", "Group No.1", "My Consumer Name 1");
        startConsumerGroup("order.new", "Group No.1", "My Consumer Name 2");
        startConsumerGroup("order.new", "Group No.2", "My Consumer Name 3");
        startConsumerGroup("order.new", "Group No.2", "My Consumer Name 4");
        startConsumerGroup("order.new", "Group No.2", "My Consumer Name 5");
        startConsumerGroup("order.new", "Group No.3", "My Consumer Name 6");

        startConsumerGroup("local.order.new", "Group No.3", "My Consumer Name 6");
        startConsumerGroup("local.order.new", "Group No.3", "My Consumer Name 6");

        startConsumerGroup("test.topic", "Group No.3", "My Consumer Name 6");

        waitForAnyKey();
    }

    private void startConsumerGroup(final String topic, final String group, String consumerName) {

        MockConsumers.getInstance().putOneConsumer(topic, group, consumerName);

        MessageChannelManager cm = lookup(MessageChannelManager.class, LocalMessageChannelManager.ID);
        ConsumerChannel cc = cm.newConsumerChannel(topic, group);
        cc.start(new ConsumerChannelHandler() {
            @Override
            public void handle(List<StoredMessage<byte[]>> msgs) {
                for (StoredMessage<byte[]> msg : msgs) {
                    MockConsumers.getInstance().consumeOneMsg(topic, group, msg);
                }
            }
            @Override
            public boolean isOpen() {
                return true;
            }
        });
    }
}