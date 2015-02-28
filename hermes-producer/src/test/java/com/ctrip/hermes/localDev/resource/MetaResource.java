package com.ctrip.hermes.localDev.resource;


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.codehaus.plexus.DefaultPlexusContainer;
import org.codehaus.plexus.PlexusContainer;

import com.ctrip.hermes.channel.MessageQueueMonitor;
import com.ctrip.hermes.localDev.LocalDevServer;

@Path("/meta")
public class MetaResource {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public MessageQueueMonitor.MessageQueueStatus getIt() throws Exception {
        return new LocalDevServer().getQueueStatus();
    }
}
