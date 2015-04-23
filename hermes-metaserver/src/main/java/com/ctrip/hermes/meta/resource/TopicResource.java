package com.ctrip.hermes.meta.resource;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.codehaus.plexus.util.StringUtils;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.dal.meta.Schema;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.pojo.SchemaView;
import com.ctrip.hermes.meta.pojo.TopicView;
import com.ctrip.hermes.meta.server.RestException;
import com.ctrip.hermes.meta.service.SchemaService;
import com.ctrip.hermes.meta.service.TopicService;

@Path("/topics/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class TopicResource {

	private TopicService topicService = PlexusComponentLocator.lookup(TopicService.class);

	private SchemaService schemaService = PlexusComponentLocator.lookup(SchemaService.class);

	public TopicResource() {
		System.out.println("Topic " + this.getClass().getClassLoader());
	}

	@POST
	public Response createTopic(String content) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}
		TopicView topicView = null;
		try {
			topicView = JSON.parseObject(content, TopicView.class);
		} catch (Exception e) {
			throw new RestException(e, Status.BAD_REQUEST);
		}

		Topic topic = topicView.toMetaTopic();

		if (topicService.getTopic(topic.getName()) != null) {
			throw new RestException("Topic already exists.", Status.CONFLICT);
		}
		try {
			topic = topicService.createTopic(topic);
			topicView = new TopicView(topic);
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).entity(topicView).build();
	}

	@GET
	public List<TopicView> findTopics(@QueryParam("pattern") String pattern) {
		if (StringUtils.isEmpty(pattern)) {
			pattern = ".*";
		}

		List<Topic> topics = topicService.findTopics(pattern);
		List<TopicView> returnResult = new ArrayList<TopicView>();
		try {
			for (Topic topic : topics) {
				TopicView topicView = new TopicView(topic);
				Storage storage = topicService.findStorage(topic.getName());
				topicView.setStorage(storage);
				if (topic.getSchemaId() != null) {
					try {
						SchemaView schemaView = schemaService.getSchemaView(topic.getSchemaId());
						topicView.setSchema(schemaView);
					} catch (DalNotFoundException e) {
					}
				}
				returnResult.add(topicView);
			}
		} catch (DalException | IOException | RestClientException e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return returnResult;
	}

	@GET
	@Path("{name}")
	public TopicView getTopic(@PathParam("name") String name) {
		Topic topic = topicService.getTopic(name);
		if (topic == null) {
			throw new RestException("Topic not found: " + name, Status.NOT_FOUND);
		}

		TopicView topicView = new TopicView(topic);

		Storage storage = topicService.findStorage(topic.getName());
		topicView.setStorage(storage);
		if (topic.getSchemaId() != null) {
			SchemaView schemaView;
			try {
				schemaView = schemaService.getSchemaView(topic.getSchemaId());
				topicView.setSchema(schemaView);
			} catch (DalException | IOException | RestClientException e) {
				throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
			}
		}

		return topicView;
	}

	@GET
	@Path("{name}/schemas")
	public List<SchemaView> getSchemas(@PathParam("name") String name) {
		List<SchemaView> returnResult = new ArrayList<SchemaView>();
		TopicView topic = getTopic(name);
		if (topic.getSchema() != null) {
			try {
				List<Schema> schemaMetas = schemaService.listSchemaView(topic.toMetaTopic());
				for (Schema schema : schemaMetas) {
					SchemaView schemaView = new SchemaView(schema);
					returnResult.add(schemaView);
				}
			} catch (DalException e) {
				throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
			}
		}
		return returnResult;
	}

	@PUT
	@Path("{name}")
	public Response updateTopic(@PathParam("name") String name, String content) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP PUT body is empty", Status.BAD_REQUEST);
		}
		TopicView topicView = null;
		try {
			topicView = JSON.parseObject(content, TopicView.class);
			topicView.setName(name);
		} catch (Exception e) {
			throw new RestException(e, Status.BAD_REQUEST);
		}

		Topic topic = topicView.toMetaTopic();

		if (topicService.getTopic(topic.getName()) == null) {
			throw new RestException("Topic does not exists.", Status.NOT_FOUND);
		}
		try {
			topic = topicService.updateTopic(topic);
			topicView = new TopicView(topic);
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(topicView).build();
	}

	@DELETE
	@Path("{name}")
	public Response deleteTopic(@PathParam("name") String name) {
		try {
			topicService.deleteTopic(name);
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}
}
