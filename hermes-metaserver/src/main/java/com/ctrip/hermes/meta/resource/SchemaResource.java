package com.ctrip.hermes.meta.resource;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.service.SchemaService;

@Path("/codec/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SchemaResource {

	private static SchemaService schemaService = PlexusComponentLocator.lookup(SchemaService.class);

	@GET
	@Path("{id}")
	public Codec get(@PathParam("id") String id) {
		Codec codec = schemaService.getCodec(id);
		return codec;
	}
}
