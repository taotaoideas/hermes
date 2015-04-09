package com.ctrip.hermes.meta.resource;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.codehaus.plexus.util.StringUtils;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.server.RestException;
import com.ctrip.hermes.meta.service.ServerMetaManager;

@Path("/meta/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetaResource {

	private static MetaManager metaManager = PlexusComponentLocator.lookup(MetaManager.class, ServerMetaManager.ID);

	@GET
	@Path("")
	public Response getMeta(@QueryParam("hashCode") long hashCode) {
		Meta meta = metaManager.getMeta();
		if (meta == null) {
			throw new RestException("Meta not found", Status.NOT_FOUND);
		}
		if (meta.hashCode() == hashCode) {
			return Response.status(Status.NOT_MODIFIED).build();
		}
		return Response.status(Status.OK).entity(meta).build();
	}

	@POST
	@Path("")
	public Response updateMeta(String content) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}
		
		Meta meta = JSON.parseObject(content, Meta.class);
		try {
			boolean result = metaManager.updateMeta(meta);
			if (result == false) {
				return Response.status(Status.NOT_MODIFIED).build();
			}
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).entity(meta).build();
	}
}
