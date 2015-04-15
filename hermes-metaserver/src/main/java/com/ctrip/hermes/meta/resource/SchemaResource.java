package com.ctrip.hermes.meta.resource;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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

	/**
	 * 
	 * @param schemaInputStream
	 * @param schemaHeader
	 * @param jarInputStream
	 * @param jarHeader
	 * @param content
	 * @param topicId
	 * @return
	 */
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response createSchema(@FormDataParam("schema-content") InputStream schemaInputStream,
	      @FormDataParam("schema-content") FormDataContentDisposition schemaHeader,
	      @FormDataParam("jar-content") InputStream jarInputStream,
	      @FormDataParam("jar-content") FormDataContentDisposition jarHeader, @FormDataParam("schema") String content,
	      @FormDataParam("topicId") @DefaultValue("0") long topicId) {
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
			schema = schemaService.createSchema(schema, topicId);
			if (schema.getType().equals("json")) {
				schemaService.uploadJsonSchema(schema, schemaInputStream, schemaHeader, jarInputStream, jarHeader);
			} else if (schema.getType().equals("avro")) {
				schemaService.uploadAvroSchema(schema, schemaInputStream, schemaHeader, jarInputStream, jarHeader);
			}
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).entity(schema).build();
	}

	/**
	 * 
	 * @param schemaId
	 * @return
	 */
	@DELETE
	@Path("{id}")
	public Response deleteSchema(@PathParam("id") long schemaId) {
		try {
			schemaService.deleteSchema(schemaId);
		} catch (DalException e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	/**
	 * 
	 * @param schemaId
	 * @return
	 */
	@GET
	@Path("{id}/schema")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response downloadSchema(@PathParam("id") long schemaId) {
		Schema schema = null;
		try {
			schema = schemaService.getSchemaMeta(schemaId);
		} catch (DalNotFoundException e) {
			throw new RestException("Schema not found: " + schemaId, Status.NOT_FOUND);
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}

		String fileProperties = schema.getSchemaProperties();
		if (StringUtils.isEmpty(fileProperties)) {
			throw new RestException("Schema file not found: " + schemaId, Status.NOT_FOUND);
		}

		return Response.status(Status.OK).header("content-disposition", fileProperties).entity(schema.getSchemaContent())
		      .build();
	}

	/**
	 * 
	 * @param schemaId
	 * @return
	 */
	@GET
	@Path("{id}/jar")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response downloadJar(@PathParam("id") long schemaId) {
		Schema schema = null;
		try {
			schema = schemaService.getSchemaMeta(schemaId);
		} catch (DalNotFoundException e) {
			throw new RestException("Schema not found: " + schemaId, Status.NOT_FOUND);
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}

		String fileProperties = schema.getJarProperties();
		if (StringUtils.isEmpty(fileProperties)) {
			throw new RestException("Schema file not found: " + schemaId, Status.NOT_FOUND);
		}

		return Response.status(Status.OK).header("content-disposition", fileProperties).entity(schema.getJarContent())
		      .build();
	}

	/**
	 * 
	 * @param name
	 * @return
	 */
	@GET
	@Path("{id}")
	public SchemaView getSchema(@PathParam("id") long schemaId) {
		SchemaView schema = null;
		try {
			schema = schemaService.getSchemaView(schemaId);
		} catch (DalNotFoundException e) {
			throw new RestException("Schema not found: " + schemaId, Status.NOT_FOUND);
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return schema;
	}

	/**
	 * 
	 * @param schemaName
	 * @return
	 */
	@GET
	public List<SchemaView> findSchemas(@QueryParam("name") String schemaName) {
		List<SchemaView> returnResult = new ArrayList<SchemaView>();
		try {
			List<Schema> schemaMetas = schemaService.findSchemaMeta(schemaName);
			for (Schema schema : schemaMetas) {
				SchemaView schemaView = new SchemaView(schema);
				returnResult.add(schemaView);
			}
		} catch (DalException e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return returnResult;
	}

	/**
	 * 
	 * @param schemaId
	 * @param content
	 * @return
	 */
	@PUT
	@Path("{id}")
	public Response updateSchema(@PathParam("id") long schemaId, String content) {
		SchemaView schema = null;
		try {
			schema = JSON.parseObject(content, SchemaView.class);
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

	/**
	 * 
	 * @param schemaId
	 * @param schemaInputStream
	 * @param schemaHeader
	 * @param jarInputStream
	 * @param jarHeader
	 * @return
	 */
	@POST
	@Path("{id}/upload")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadFile(@PathParam("id") long schemaId,
	      @FormDataParam("schema-content") InputStream schemaInputStream,
	      @FormDataParam("schema-content") FormDataContentDisposition schemaHeader,
	      @FormDataParam("jar-content") InputStream jarInputStream,
	      @FormDataParam("jar-content") FormDataContentDisposition jarHeader) {
		SchemaView schemaView = getSchema(schemaId);
		try {
			if (schemaView.getType().equals("json")) {
				schemaService.uploadJsonSchema(schemaView, schemaInputStream, schemaHeader, jarInputStream, jarHeader);
			} else if (schemaView.getType().equals("avro")) {
				schemaService.uploadAvroSchema(schemaView, schemaInputStream, schemaHeader, jarInputStream, jarHeader);
			}
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).build();
	}
}
