package com.ctrip.hermes.meta.resource;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.server.RestException;

@Path("/meta/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetaResource {

	private static MetaManager metaManager = PlexusComponentLocator.lookup(MetaManager.class);

	@GET
	@Path("")
	public Meta getMeta() {
		Meta meta = metaManager.getMeta();
		if (meta == null) {
			throw new RestException("Meta not found", Status.NOT_FOUND);
		}
		return meta;
	}
}
