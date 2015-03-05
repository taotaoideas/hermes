package com.ctrip.hermes.local.resource;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.codehaus.plexus.PlexusContainer;
import org.unidal.lookup.ContainerLoader;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.channel.MessageQueueMonitor;
import com.ctrip.hermes.local.MockConsumers;
import com.ctrip.hermes.local.pojo.MockConsumer;
import com.ctrip.hermes.local.pojo.MockConsumerGroup;
import com.ctrip.hermes.local.pojo.OutputMessage;
import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.storage.Locatable;
import com.ctrip.hermes.storage.storage.Offset;

@Path("/consumer")
public class ConsumerResource {
    PlexusContainer container = ContainerLoader.getDefaultContainer();


    @GET
    @Path("/old")
    @Produces(MediaType.APPLICATION_JSON)
    public List<MockConsumerGroup> oldGetDetails(@QueryParam("topic") String topic) {
        if (null == topic) {

            return MockConsumers.getInstance().getAllGroup();
        } else {
            return MockConsumers.getInstance().getGroupByTopic(topic);
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<MockConsumerGroup> getDetails(@QueryParam("topic") String topic) throws Exception {
        MessageQueueMonitor.MessageQueueStatus status = container.lookup(MessageQueueMonitor.class).status();

        return buildConsumerGroup(status);
    }

    private List<MockConsumerGroup> buildConsumerGroup(MessageQueueMonitor.MessageQueueStatus status) {
        List<MockConsumerGroup> result = new ArrayList<>();
        List<Pair<MessageQueueMonitor.ConsumerStatus<Record>,
                MessageQueueMonitor.ConsumerStatus<Resend>>> consumers = status.getConsumers();
        Map<String, List<Record>> topics = status.getTopics();

        for (Pair<MessageQueueMonitor.ConsumerStatus<Record>, MessageQueueMonitor.ConsumerStatus<Resend>> consumer : consumers) {
            MessageQueueMonitor.ConsumerStatus<Record> send = consumer.getKey();

            // get messages of this consumer;
            String group = send.getGroupId();
            String topic = send.getTopic();

            // remove "invalid"
            if ("invalid".equals(group)) {
                continue;
            }

            List<Offset> nearByMsgs = new ArrayList<>();
            for (Locatable locatable : send.getNearbyMessages()) {
                nearByMsgs.add(locatable.getOffset());
            }

            List<Record> records = topics.get(topic);

            List<OutputMessage> outputMessages = new ArrayList<>();
            for (Offset offset : nearByMsgs) {
                for (Record record : records) {
                    if (record.getOffset().equals(offset)) {
                        outputMessages.add(OutputMessage.convert(record));
                    }
                }
            }


            List<MockConsumer> cs = new ArrayList<>(1);
            cs.add(new MockConsumer(group, outputMessages));

            result.add(new MockConsumerGroup(topic, group, cs));
        }
        return result;
    }
}
