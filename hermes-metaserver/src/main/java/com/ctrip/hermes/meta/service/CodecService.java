package com.ctrip.hermes.meta.service;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Codec;

@Path("/codec/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class CodecService {

	private static MetaService m_meta;

	public CodecService() {
		if (m_meta == null) {
			m_meta = PlexusComponentLocator.lookup(MetaService.class);
		}
	}

	@GET
	@Path("{id}")
	public Codec get(@PathParam("id") String id) {
		Codec codec = m_meta.getCodec(id);
		return codec;
	}
}
