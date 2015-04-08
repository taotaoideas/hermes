package com.ctrip.hermes.meta.resource;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.service.TopicService;

@Path("/topic/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class TopicResource {

	private static TopicService topicService = PlexusComponentLocator.lookup(TopicService.class);

	@POST
	@Path("")
	public Response create(String content) {
		return Response.status(Status.CREATED).entity(content).build();
	}

	@GET
	@Path("{name}")
	public Topic get(@PathParam("name") String name) {
		Topic topic = topicService.getTopic(name);
		return topic;
	}

}
