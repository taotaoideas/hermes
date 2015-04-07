package com.ctrip.hermes.meta.service;

import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Topic;

@Path("/topic/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class TopicService {

	private static MetaService m_meta;

	public TopicService() {
		if (m_meta == null) {
			m_meta = PlexusComponentLocator.lookup(MetaService.class);
		}
	}

	@POST
	@Path("")
	public Response create(String content) {
		return Response.status(Status.CREATED).entity(content).build();
	}

	@GET
	@Path("{name}")
	public List<Topic> get(@PathParam("name") String name) {
		List<Topic> topics = m_meta.findTopicsByPattern(name);
		return topics;
	}

}
