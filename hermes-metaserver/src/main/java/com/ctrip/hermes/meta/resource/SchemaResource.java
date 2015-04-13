package com.ctrip.hermes.meta.resource;

import java.io.InputStream;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.codehaus.plexus.util.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.dal.meta.Schema;
import com.ctrip.hermes.meta.pojo.SchemaView;
import com.ctrip.hermes.meta.server.RestException;
import com.ctrip.hermes.meta.service.SchemaService;

@Path("/schemas/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SchemaResource {

	private static SchemaService schemaService = PlexusComponentLocator.lookup(SchemaService.class);

	@POST
	@Path("")
	public Response createSchema(String content) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}
		SchemaView schema = null;
		try {
			schema = JSON.parseObject(content, SchemaView.class);
		} catch (Exception e) {
			throw new RestException(e, Status.BAD_REQUEST);
		}
		try {
			schema = schemaService.createSchema(schema);
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).entity(schema).build();
	}

	@GET
	@Path("{name}")
	public SchemaView getSchema(@PathParam("name") String name) {
		if (StringUtils.isEmpty(name)) {
			throw new RestException("HTTP path {name} is empty", Status.BAD_REQUEST);
		}
		SchemaView schema = null;
		try {
			schema = schemaService.getSchemaView(name);
		} catch (DalNotFoundException e) {
			throw new RestException("Schema not found: " + name, Status.NOT_FOUND);
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return schema;
	}

	@PUT
	@Path("{name}")
	public Response updateSchema(@PathParam("name") String name, String content) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP PUT body is empty", Status.BAD_REQUEST);
		}

		SchemaView schema = null;
		try {
			schema = JSON.parseObject(content, SchemaView.class);
			schema.setName(name);
		} catch (Exception e) {
			throw new RestException(e, Status.BAD_REQUEST);
		}
		try {
			schema = schemaService.updateSchemaView(schema);
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).entity(schema).build();
	}

	@DELETE
	@Path("{name}")
	public Response deleteSchema(@PathParam("name") String name) {
		try {
			schemaService.deleteSchema(name);
		} catch (DalException e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	@POST
	@Path("{name}/upload")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadFile(@PathParam("name") String name, @FormDataParam("file") InputStream is,
	      @FormDataParam("file") FormDataContentDisposition header) {
		SchemaView schemaView = getSchema(name);
		try {
			if (schemaView.getType().equals("json")) {
				schemaService.uploadJson(schemaView, is, header);
			} else if (schemaView.getType().equals("avro")) {
				schemaService.uploadAvro(schemaView, is, header);
			}
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).build();
	}

	@GET
	@Path("{name}/download")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response downloadFile(@PathParam("name") String name) {
		if (StringUtils.isEmpty(name)) {
			throw new RestException("HTTP path {name} is empty", Status.BAD_REQUEST);
		}
		Schema schema = null;
		try {
			schema = schemaService.getSchemaMeta(name);
		} catch (DalNotFoundException e) {
			throw new RestException("Schema not found: " + name, Status.NOT_FOUND);
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}

		String fileProperties = schema.getFileProperties();
		if (StringUtils.isEmpty(fileProperties)) {
			throw new RestException("Schema file not found: " + name, Status.NOT_FOUND);
		}

		return Response.status(Status.OK).header("content-disposition", fileProperties).entity(schema.getFileContent())
		      .build();
	}
}
