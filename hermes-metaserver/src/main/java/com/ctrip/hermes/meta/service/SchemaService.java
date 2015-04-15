package com.ctrip.hermes.meta.service;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;

import org.apache.avro.Schema.Parser;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.dal.meta.Schema;
import com.ctrip.hermes.meta.dal.meta.SchemaDao;
import com.ctrip.hermes.meta.dal.meta.SchemaEntity;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.pojo.SchemaView;
import com.google.common.io.ByteStreams;

@Named
public class SchemaService {

	private SchemaRegistryClient avroSchemaRegistry = new CachedSchemaRegistryClient("http://10.3.8.63:8081", 1000);

	@Inject
	private SchemaDao schemaDao;

	@Inject(ServerMetaManager.ID)
	private MetaManager m_metaManager;

	@Inject
	private MetaService m_metaService;

	@Inject
	private TopicService m_topicService;

	/**
	 * 
	 * @param schemaView
	 * @return
	 * @throws DalException
	 */
	public SchemaView createSchema(SchemaView schemaView) throws DalException {
		return createSchema(schemaView, 0);
	}

	/**
	 * 
	 * @param schemaView
	 * @param topicId
	 * @return
	 * @throws DalException
	 */
	public SchemaView createSchema(SchemaView schemaView, long topicId) throws DalException {
		Schema schema = schemaView.toMetaSchema();
		schema.setCreateTime(new Date(System.currentTimeMillis()));
		schema.setVersion(1);
		schemaDao.insert(schema);

		if (topicId > 0) {
			Topic topic = m_metaService.findTopic(topicId);
			topic.setSchemaId(schema.getId());
			m_topicService.updateTopic(topic);
		}
		return new SchemaView(schema);
	}

	/**
	 * 
	 * @param id
	 * @throws DalException
	 */
	public void deleteSchema(long id) throws DalException {
		Schema schema = schemaDao.findByPK(id, SchemaEntity.READSET_FULL);
		schemaDao.deleteByPK(schema);
	}

	/**
	 * 
	 * @param schemaName
	 * @return
	 * @throws DalException
	 */
	public Schema findLatestSchemaMeta(String schemaName) throws DalException {
		Schema schema = schemaDao.findLatestByName(schemaName, SchemaEntity.READSET_FULL);
		return schema;
	}

	/**
	 * 
	 * @param schemaName
	 * @return
	 * @throws DalException
	 */
	public List<Schema> findSchemaMeta(String schemaName) throws DalException {
		List<Schema> schemas = schemaDao.findByName(schemaName, SchemaEntity.READSET_FULL);
		return schemas;
	}

	/**
	 * 
	 * @param schemaName
	 * @return
	 * @throws IOException
	 * @throws RestClientException
	 * @throws DalException
	 */
	public org.apache.avro.Schema getAvroSchema(String schemaName) throws IOException, RestClientException, DalException {
		Schema schema = schemaDao.findLatestByName(schemaName, SchemaEntity.READSET_FULL);
		if (schema.getAvroid() > 0) {
			org.apache.avro.Schema avroSchema = this.avroSchemaRegistry.getByID(schema.getAvroid());
			return avroSchema;
		}
		return null;
	}

	/**
	 * 
	 * @param schemaId
	 * @return
	 * @throws DalException
	 */
	public Schema getSchemaMeta(long schemaId) throws DalException {
		Schema schema = schemaDao.findByPK(schemaId, SchemaEntity.READSET_FULL);
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
	 * @param schemaView
	 * @return
	 * @throws DalException
	 */
	public SchemaView updateSchemaView(SchemaView schemaView) throws DalException {
		Schema schema = schemaView.toMetaSchema();
		Schema oldSchema = schemaDao.findLatestByName(schema.getName(), SchemaEntity.READSET_FULL);
		schema.setVersion(oldSchema.getVersion() + 1);
		schema.setCreateTime(new Date(System.currentTimeMillis()));
		schema.setId(0);
		schemaDao.insert(schema);
		return new SchemaView(schema);
	}

	/**
	 * 
	 * @param schemaView
	 * @param schemaInputStream
	 * @param schemaHeader
	 * @param jarInputStream
	 * @param jarHeader
	 * @throws IOException
	 * @throws DalException
	 * @throws RestClientException
	 */
	public void uploadAvroSchema(SchemaView schemaView, InputStream schemaInputStream,
	      FormDataContentDisposition schemaHeader, InputStream jarInputStream, FormDataContentDisposition jarHeader)
	      throws IOException, DalException, RestClientException {
		Schema metaSchema = schemaView.toMetaSchema();
		if (schemaInputStream != null) {
			byte[] schemaContent = ByteStreams.toByteArray(schemaInputStream);
			metaSchema.setSchemaContent(schemaContent);
			metaSchema.setSchemaProperties(schemaHeader.toString());

			Parser parser = new Parser();
			org.apache.avro.Schema avroSchema = parser.parse(new String(schemaContent));
			int avroid = avroSchemaRegistry.register(metaSchema.getName(), avroSchema);
			metaSchema.setAvroid(avroid);
		}
		if (jarInputStream != null) {
			byte[] jarContent = ByteStreams.toByteArray(jarInputStream);
			metaSchema.setJarContent(jarContent);
			metaSchema.setJarProperties(jarHeader.toString());
		}
		schemaDao.updateByPK(metaSchema, SchemaEntity.UPDATESET_FULL);
	}

	/**
	 * 
	 * @param schemaView
	 * @param schemaInputStream
	 * @param schemaHeader
	 * @param jarInputStream
	 * @param jarHeader
	 * @throws IOException
	 * @throws DalException
	 */
	public void uploadJsonSchema(SchemaView schemaView, InputStream schemaInputStream,
	      FormDataContentDisposition schemaHeader, InputStream jarInputStream, FormDataContentDisposition jarHeader)
	      throws IOException, DalException {
		Schema metaSchema = schemaView.toMetaSchema();
		if (schemaInputStream != null) {
			byte[] schemaContent = ByteStreams.toByteArray(schemaInputStream);
			metaSchema.setSchemaContent(schemaContent);
			metaSchema.setSchemaProperties(schemaHeader.toString());
		}

		if (jarInputStream != null) {
			byte[] jarContent = ByteStreams.toByteArray(jarInputStream);
			metaSchema.setJarContent(jarContent);
			metaSchema.setJarProperties(jarHeader.toString());
		}
		schemaDao.updateByPK(metaSchema, SchemaEntity.UPDATESET_FULL);
	}

}
