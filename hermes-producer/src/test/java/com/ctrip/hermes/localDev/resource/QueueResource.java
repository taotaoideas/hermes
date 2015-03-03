package com.ctrip.hermes.localDev.resource;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.codehaus.plexus.PlexusContainer;
import org.unidal.lookup.ContainerLoader;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.channel.MessageQueueMonitor;
import com.ctrip.hermes.message.MessagePackage;
import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.storage.Offset;

@Path("/queue")
public class QueueResource {
    PlexusContainer container = ContainerLoader.getDefaultContainer();

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<OutputMessage> getMessages(@QueryParam("topic") String topic) throws Exception {
        Map<String, List<Message>> topicMap = container.lookup(MessageQueueMonitor.class).status().getTopics();
        if (topicMap.containsKey(topic)) {
             List<OutputMessage> result = buildOutputMessage(topicMap.get(topic));
            return result;
        } else {
            return null;
        }
    }

    private List<OutputMessage> buildOutputMessage(List<Message> messages) {
        List<OutputMessage> result = new ArrayList<>();
        for (Message msg : messages) {

            MessagePackage pkg = JSON.parseObject(((Message) msg).getContent(),
                    MessagePackage.class);
            String body = null, key = null;
            if (null != pkg) {
                body = JSON.parseObject(pkg.getMessage(), String.class);
                key = JSON.parseObject(pkg.getKey(), String.class);
            }
            result.add(new OutputMessage(body, key, msg.getOffset(), msg.getAckOffset(), msg.getProperties()));
        }
        return result;
    }


    private class OutputMessage {

        public String message;
        public String key;
        public Offset offset;
        public Offset ackOffset;
        public Map<String, String> properties = new HashMap<String, String>();

        public OutputMessage(String message, String key, Offset offset, Offset ackOffset, Map<String, String> properties) {
            this.message = message;
            this.key = key;
            this.offset = offset;
            this.ackOffset = ackOffset;
            this.properties = properties;
        }
    }
}
