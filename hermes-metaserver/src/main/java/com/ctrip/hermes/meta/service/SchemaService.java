package com.ctrip.hermes.meta.service;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema.Parser;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.lookup.util.StringUtils;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.dal.meta.Schema;
import com.ctrip.hermes.meta.dal.meta.SchemaDao;
import com.ctrip.hermes.meta.dal.meta.SchemaEntity;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.pojo.SchemaView;

import ctrip.io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import ctrip.io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

@Named
public class SchemaService {

	private static final String DEFAULT_COMPABILITY = "FORWARD";

	private SchemaRegistryClient avroSchemaRegistry;

	@Inject
	private ClientEnvironment m_env;

	@Inject
	private SchemaDao m_schemaDao;

	@Inject(ServerMetaManager.ID)
	private MetaManager m_metaManager;

	@Inject(ServerMetaService.ID)
	private MetaService m_metaService;

	@Inject
	private TopicService m_topicService;

	@Inject
	private CompileService m_compileService;

	/**
	 * 
	 * @param schemaName
	 * @param schemaContent
	 * @throws IOException
	 * @throws RestClientException
	 */
	public void checkAvroSchema(String schemaName, byte[] schemaContent) throws IOException, RestClientException {
		Parser parser = new Parser();
		org.apache.avro.Schema avroSchema = parser.parse(new String(schemaContent));
		getAvroSchemaRegistry().register(schemaName, avroSchema);
	}

	/**
	 * 
	 * @param metaSchema
	 * @param avroSchema
	 * @throws IOException
	 * @throws DalException
	 */
	public void compileAvro(Schema metaSchema, org.apache.avro.Schema avroSchema) throws IOException, DalException {
		final Path destDir = Files.createTempDirectory("avroschema");
		SpecificCompiler compiler = new SpecificCompiler(avroSchema);
		compiler.compileToDestination(null, destDir.toFile());

		m_compileService.compile(destDir);
		Path jarFile = Files.createTempFile(metaSchema.getName(), ".jar");
		m_compileService.jar(destDir, jarFile);

		byte[] jarContent = Files.readAllBytes(jarFile);
		metaSchema.setJarContent(jarContent);
		FormDataContentDisposition disposition = FormDataContentDisposition.name(metaSchema.getName())
		      .creationDate(new Date(System.currentTimeMillis()))
		      .fileName(metaSchema.getName() + "_" + metaSchema.getVersion() + ".jar").size(jarFile.toFile().length())
		      .build();
		metaSchema.setJarProperties(disposition.toString());
		m_schemaDao.updateByPK(metaSchema, SchemaEntity.UPDATESET_FULL);
		Files.delete(jarFile);
		m_compileService.delete(destDir);
	}

	/**
	 * 
	 * @param schemaView
	 * @param topicId
	 * @return
	 * @throws DalException
	 * @throws RestClientException
	 * @throws IOException
	 */
	public SchemaView createSchema(SchemaView schemaView, Topic topic) throws DalException, IOException,
	      RestClientException {
		Schema schema = schemaView.toMetaSchema();
		schema.setCreateTime(new Date(System.currentTimeMillis()));
		schema.setName(topic.getName() + "-value");
		schema.setTopicId(topic.getId());
		try {
			Schema maxVersionSchemaMeta = m_schemaDao.getMaxVersionByTopic(topic.getId(), SchemaEntity.READSET_FULL);
			schema.setVersion(maxVersionSchemaMeta.getVersion() + 1);
		} catch (DalNotFoundException e) {
			schema.setVersion(1);
		}
		m_schemaDao.insert(schema);

		topic.setSchemaId(schema.getId());
		m_topicService.updateTopic(topic);

		if ("avro".equals(schema.getType())) {
			if (StringUtils.isEmpty(schema.getCompatibility())) {
				schema.setCompatibility(DEFAULT_COMPABILITY);
			}
			getAvroSchemaRegistry().updateCompatibility(schema.getName(), schema.getCompatibility());
		}

		return new SchemaView(schema);
	}

	/**
	 * 
	 * @param id
	 * @param oldSchemaId
	 * @throws DalException
	 */
	public void deleteSchema(long id, Long oldSchemaId) throws DalException {
		Schema schema = m_schemaDao.findByPK(id, SchemaEntity.READSET_FULL);
		Topic topic = m_topicService.getTopic(schema.getTopicId());
		topic.setSchemaId(oldSchemaId);
		m_topicService.updateTopic(topic);
		m_schemaDao.deleteByPK(schema);
	}

	/**
	 * 
	 * @param topic
	 * @throws DalException
	 */
	public void deleteSchemas(Topic topic) throws DalException {
		List<Schema> schemas = m_schemaDao.findByTopic(topic.getId(), SchemaEntity.READSET_FULL);
		for (Schema schema : schemas) {
			m_schemaDao.deleteByPK(schema);
		}
	}

	private SchemaRegistryClient getAvroSchemaRegistry() throws IOException {
		if (avroSchemaRegistry == null) {
			Properties m_properties = m_env.getGlobalConfig();
			String schemaServerHost = m_properties.getProperty("schema-server-host");
			String schemaServerPort = m_properties.getProperty("schema-server-port");
			avroSchemaRegistry = new CachedSchemaRegistryClient("http://" + schemaServerHost + ":" + schemaServerPort,
			      1000);
		}
		return avroSchemaRegistry;
	}

	/**
	 * 
	 * @param schema
	 * @return
	 * @throws IOException
	 * @throws RestClientException
	 */
	public String getCompatible(Schema schema) throws IOException, RestClientException {
		return getAvroSchemaRegistry().getCompatibility(schema.getName());
	}

	/**
	 * 
	 * @param schemaId
	 * @return
	 * @throws DalException
	 */
	public Schema getSchemaMeta(long schemaId) throws DalException {
		Schema schema = m_schemaDao.findByPK(schemaId, SchemaEntity.READSET_FULL);
		return schema;
	}

	/**
	 * 
	 * @param schemaId
	 * @return
	 * @throws DalException
	 * @throws IOException
	 * @throws RestClientException
	 */
	public SchemaView getSchemaView(long schemaId) throws DalException, IOException, RestClientException {
		Schema schema = getSchemaMeta(schemaId);
		SchemaView schemaView = new SchemaView(schema);
		return schemaView;
	}

	/**
	 * 
	 * @return
	 * @throws DalException
	 */
	public List<Schema> listLatestSchemaMeta() throws DalException {
		List<Schema> schemas = m_schemaDao.listLatest(SchemaEntity.READSET_FULL);
		return schemas;
	}

	/**
	 * 
	 * @param topic
	 * @return
	 * @throws DalException
	 */
	public List<Schema> listSchemaView(Topic topic) throws DalException {
		List<Schema> schemas = m_schemaDao.findByTopic(topic.getId(), SchemaEntity.READSET_FULL);
		return schemas;
	}

	/**
	 * 
	 * @param schemaView
	 * @param fileContent
	 * @param fileHeader
	 * @return
	 * @throws IOException
	 * @throws DalException
	 * @throws RestClientException
	 */
	public SchemaView updateSchemaFile(SchemaView schemaView, byte[] fileContent, FormDataContentDisposition fileHeader)
	      throws IOException, DalException, RestClientException {
		SchemaView result = null;
		if (schemaView.getType().equals("json")) {
			result = uploadJsonSchema(schemaView, null, null, fileContent, fileHeader);
		} else if (schemaView.getType().equals("avro")) {
			result = uploadAvroSchema(schemaView, fileContent, fileHeader, null, null);
		}
		return result;
	}

	/**
	 * 
	 * @param schemaView
	 * @param schemaContent
	 * @param schemaHeader
	 * @param jarContent
	 * @param jarHeader
	 * @throws IOException
	 * @throws DalException
	 * @throws RestClientException
	 */
	public SchemaView uploadAvroSchema(SchemaView schemaView, byte[] schemaContent,
	      FormDataContentDisposition schemaHeader, byte[] jarContent, FormDataContentDisposition jarHeader)
	      throws IOException, DalException, RestClientException {
		if (schemaContent == null) {
			return schemaView;
		}

		boolean isUpdated = false;
		Schema metaSchema = schemaView.toMetaSchema();
		if (schemaContent != null) {
			metaSchema.setSchemaContent(schemaContent);
			metaSchema.setSchemaProperties(schemaHeader.toString());

			Parser parser = new Parser();
			org.apache.avro.Schema avroSchema = parser.parse(new String(schemaContent));
			int avroid = getAvroSchemaRegistry().register(metaSchema.getName(), avroSchema);
			metaSchema.setAvroid(avroid);

			compileAvro(metaSchema, avroSchema);
			isUpdated = true;
		}
		if (jarContent != null) {
			metaSchema.setJarContent(jarContent);
			metaSchema.setJarProperties(jarHeader.toString());
			isUpdated = true;
		}

		if (isUpdated) {
			m_schemaDao.updateByPK(metaSchema, SchemaEntity.UPDATESET_FULL);
			return new SchemaView(metaSchema);
		} else {
			return schemaView;
		}
	}

	/**
	 * 
	 * @param schemaView
	 * @param schemaContent
	 * @param schemaHeader
	 * @param jarContent
	 * @param jarHeader
	 * @throws IOException
	 * @throws DalException
	 */
	public SchemaView uploadJsonSchema(SchemaView schemaView, byte[] schemaContent,
	      FormDataContentDisposition schemaHeader, byte[] jarContent, FormDataContentDisposition jarHeader)
	      throws IOException, DalException {
		if (schemaContent == null) {
			return schemaView;
		}

		boolean isUpdated = false;
		Schema metaSchema = schemaView.toMetaSchema();
		if (schemaContent != null) {
			metaSchema.setSchemaContent(schemaContent);
			metaSchema.setSchemaProperties(schemaHeader.toString());
			isUpdated = true;
		}

		if (jarContent != null) {
			metaSchema.setJarContent(jarContent);
			metaSchema.setJarProperties(jarHeader.toString());
			isUpdated = true;
		}

		if (isUpdated) {
			m_schemaDao.updateByPK(metaSchema, SchemaEntity.UPDATESET_FULL);
			return new SchemaView(metaSchema);
		} else {
			return schemaView;
		}
	}

	/**
	 * 
	 * @param name
	 * @param schemaContent
	 * @return
	 * @throws IOException
	 * @throws RestClientException
	 */
	public boolean verifyCompatible(Schema schema, byte[] schemaContent) throws IOException, RestClientException {
		if (schemaContent == null) {
			return false;
		}
		Parser parser = new Parser();
		org.apache.avro.Schema avroSchema = parser.parse(new String(schemaContent));
		boolean result = getAvroSchemaRegistry().testCompatibility(schema.getName(), avroSchema);
		return result;
	}

}
