package com.ctrip.hermes.localDev.resource;


import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.codehaus.plexus.PlexusContainer;
import org.unidal.lookup.ContainerLoader;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.channel.MessageQueueMonitor;
import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.message.Resend;

@Path("/consumer")
public class ConsumerResource {
    PlexusContainer container = ContainerLoader.getDefaultContainer();

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<ConsumerStatus> getMessages(@QueryParam("topic") String topic) throws Exception {
        List<Pair<MessageQueueMonitor.ConsumerStatus<Message>, MessageQueueMonitor.ConsumerStatus<Resend>>>
                consumers = container.lookup(MessageQueueMonitor.class).status().getConsumers();

        System.out.println("Consumers:" + consumers.size());

        List<ConsumerStatus> result = buildConsumerStatus(consumers, topic);
        return result;
    }

    private List<ConsumerStatus> buildConsumerStatus(
            List<Pair<MessageQueueMonitor.ConsumerStatus<Message>,
                    MessageQueueMonitor.ConsumerStatus<Resend>>> consumers, String topic) {
        List<ConsumerStatus> result = new ArrayList<>();
        for (Pair<MessageQueueMonitor.ConsumerStatus<Message>, MessageQueueMonitor.ConsumerStatus<Resend>> consumer : consumers) {
            if (consumer.getKey().getTopic().equals(topic)) {  //check topic
                String group = consumer.getKey().getGroupId();
                long sendNextOffset = consumer.getKey().getNextConsumeOffset();
                long sendTopOffset = consumer.getKey().getTopOffset();
                long resendNextOffset = consumer.getValue().getNextConsumeOffset();
                long resendTopOffset = consumer.getValue().getTopOffset();

                result.add(new ConsumerStatus(group, sendNextOffset, sendTopOffset, resendNextOffset,
                        resendTopOffset));
            }
        }
        return result;
    }

    private class ConsumerStatus {
        public String group;
        public long sendNextOffset;
        public long sendTopOffset;
        public long resendNextOffset;
        public long resendTopOffset;

        public ConsumerStatus(String group, long sendNextOffset, long sendTopOffset, long resendNextOffset, long resendTopOffset) {
            this.group = group;
            this.sendNextOffset = sendNextOffset;
            this.sendTopOffset = sendTopOffset;
            this.resendNextOffset = resendNextOffset;
            this.resendTopOffset = resendTopOffset;
        }
    }
}
