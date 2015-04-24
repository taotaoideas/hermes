package com.ctrip.hermes.meta.resource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.pojo.CodecView;
import com.ctrip.hermes.meta.server.RestException;
import com.ctrip.hermes.meta.service.CodecService;

@Path("/codecs/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class CodecResource {

	private CodecService codecService = PlexusComponentLocator.lookup(CodecService.class);

	private ClientEnvironment m_env = PlexusComponentLocator.lookup(ClientEnvironment.class);

	@GET
	@Path("{name}")
	public CodecView getCodec(@PathParam("name") String name) {
		Codec codec = codecService.getCodec(name);
		if (codec == null) {
			throw new RestException("Codec not found: " + name, Status.NOT_FOUND);
		}
		return new CodecView(codec);
	}

	@GET
	public List<CodecView> listCodecs() throws IOException {
		// FIXME hard code two codecs
		List<CodecView> result = new ArrayList<>();
		CodecView jsonCodec = new CodecView();
		jsonCodec.setType("json");
		CodecView avroCodec = new CodecView();
		avroCodec.setType("avro");
		List<Property> properties = new ArrayList<>();
		Property prop = new Property();
		prop.setName("schema.registry.url");
		Properties globalConfig = m_env.getGlobalConfig();
		String schemaServerHost = globalConfig.getProperty("schema-server-host");
		String schemaServerPort = globalConfig.getProperty("schema-server-port");
		prop.setValue("http://" + schemaServerHost + ":" + schemaServerPort);
		avroCodec.setProperties(properties);
		result.add(jsonCodec);
		result.add(avroCodec);
		return result;
	}
}
