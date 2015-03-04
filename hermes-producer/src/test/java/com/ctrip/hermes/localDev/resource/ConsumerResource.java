package com.ctrip.hermes.localDev.resource;


import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.codehaus.plexus.PlexusContainer;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.hermes.localDev.MockConsumers;

@Path("/consumer")
public class ConsumerResource {
    PlexusContainer container = ContainerLoader.getDefaultContainer();


    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<MockConsumers.MockConsumerGroup> getDetails(@QueryParam("topic") String topic)  {
        if (null == topic) {

            return MockConsumers.getInstance().getAllGroup();
        } else {
            return MockConsumers.getInstance().getGroupByTopic(topic);
        }
    }
}
