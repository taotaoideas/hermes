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

import com.ctrip.hermes.channel.MessageQueueMonitor;
import com.ctrip.hermes.local.pojo.OutputMessage;
import com.ctrip.hermes.storage.message.Record;

@Path("/queue")
public class QueueResource {
    PlexusContainer container = ContainerLoader.getDefaultContainer();

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<OutputMessage> getMessages(@QueryParam("topic") String topic) throws Exception {
        Map<String, List<Record>> topicMap = container.lookup(MessageQueueMonitor.class).status().getTopics();
        if (topicMap.containsKey(topic)) {
             List<OutputMessage> result = buildOutputMessage(topicMap.get(topic));
            return result;
        } else {
            return null;
        }
    }

    private List<OutputMessage> buildOutputMessage(List<Record> messages) {
        List<OutputMessage> result = new ArrayList<>();
        for (Record msg : messages) {
            result.add(OutputMessage.convert(msg));
        }
        return result;
    }
}
