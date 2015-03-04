package com.ctrip.hermes.localDev.resource;


import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.codehaus.plexus.PlexusContainer;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.hermes.channel.MessageQueueMonitor;

@Path("/meta")
public class MetaResource {
    PlexusContainer container = ContainerLoader.getDefaultContainer();

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public MessageQueueMonitor.MessageQueueStatus getMeta() throws Exception {
        return container.lookup(MessageQueueMonitor.class).status();
    }

    @GET
    @Path("/topic")
    @Produces(MediaType.APPLICATION_JSON)
    public Set<String> gettopic() throws Exception {
        return container.lookup(MessageQueueMonitor.class).status().getTopics().keySet();
    }
}
