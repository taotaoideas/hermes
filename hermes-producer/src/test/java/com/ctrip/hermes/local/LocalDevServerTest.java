package com.ctrip.hermes.local;

import java.util.List;

import org.junit.Test;
import org.mockito.cglib.core.Local;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.channel.ConsumerChannel;
import com.ctrip.hermes.channel.ConsumerChannelHandler;
import com.ctrip.hermes.channel.LocalMessageChannelManager;
import com.ctrip.hermes.channel.MessageChannelManager;
import com.ctrip.hermes.message.StoredMessage;

public class LocalDevServerTest extends ComponentTestCase {

    @Test
    public void test() throws Exception {
        LocalDevServer.getInstance().start();
        startWebapp();

        System.in.read();
    }

    public static void main(String[] args) throws Exception {
        LocalDevServer.getInstance().start();
    }

    public void startWebapp() throws Exception {


        startConsumerGroup("order.new", "Group No.1", "My Consumer Name 1");
        startConsumerGroup("order.new", "Group No.1", "My Consumer Name 2");
        startConsumerGroup("order.new", "Group No.2", "My Consumer Name 3");
        startConsumerGroup("order.new", "Group No.2", "My Consumer Name 4");
        startConsumerGroup("order.new", "Group No.2", "My Consumer Name 5");
        startConsumerGroup("order.new", "Group No.3", "My Consumer Name 6");

        startConsumerGroup("local.order.new", "Group No.3", "My Consumer Name 6");
        startConsumerGroup("local.order.new", "Group No.3", "My Consumer Name 6");

        startConsumerGroup("test.topic", "Group No.3", "My Consumer Name 6");
    }

    private void startConsumerGroup(final String topic, final String group, String consumerName) {

    MockConsumers.getInstance().putOneConsumer(topic, group, consumerName);

    ConsumerChannel cc = lookup(MessageChannelManager.class, LocalMessageChannelManager.ID).newConsumerChannel(topic,
            group);
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
