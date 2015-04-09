package com.ctrip.hermes.meta.resource;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.pojo.SchemaView;
import com.ctrip.hermes.meta.server.RestException;
import com.ctrip.hermes.meta.service.SchemaService;

@Path("/schemas/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SchemaResource {

	private static SchemaService schemaService = PlexusComponentLocator.lookup(SchemaService.class);

	@GET
	@Path("{name}")
	public SchemaView getSchema(@PathParam("name") String name) {
		SchemaMetadata latestAvroSchemaMetadata = schemaService.getLatestAvroSchemaMetadata(name);
		if (latestAvroSchemaMetadata == null) {
			throw new RestException("schema not found: " + name, Status.NOT_FOUND);
		}
		return new SchemaView(latestAvroSchemaMetadata);
	}
}
