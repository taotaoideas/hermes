package com.ctrip.hermes.meta.service;

//import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
//import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
//import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
//import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.dal.meta.Schema;
import com.ctrip.hermes.meta.dal.meta.SchemaDao;
import com.ctrip.hermes.meta.dal.meta.SchemaEntity;
import com.ctrip.hermes.meta.pojo.SchemaView;
import com.google.common.io.ByteStreams;

@Named
public class SchemaService {

	private SchemaRegistryClient avroSchemaRegistry = new CachedSchemaRegistryClient("http://10.3.8.63:8081", 1000);

	@Inject
	private SchemaDao schemaDao;

	public void createAvroSchema(String schemaName, org.apache.avro.Schema avroSchema) throws IOException,
	      RestClientException, DalException {
		Schema schema = schemaDao.findLatestByName(schemaName, SchemaEntity.READSET_FULL);
		int avroid = this.avroSchemaRegistry.register(schemaName, avroSchema);
		schema.setAvroid(avroid);
		schemaDao.updateByPK(schema, SchemaEntity.UPDATESET_FULL);
	}

	public SchemaView createSchema(SchemaView schemaView) throws DalException {
		Schema schema = schemaView.toMetaSchema();
		schema.setCreateTime(new Date(System.currentTimeMillis()));
		schema.setVersion(1);
		schemaDao.insert(schema);
		return new SchemaView(schema);
	}

	public org.apache.avro.Schema getAvroSchema(String schemaName) throws IOException, RestClientException, DalException {
		Schema schema = schemaDao.findLatestByName(schemaName, SchemaEntity.READSET_FULL);
		if (schema.getAvroid() > 0) {
			org.apache.avro.Schema avroSchema = this.avroSchemaRegistry.getByID(schema.getAvroid());
			return avroSchema;
		}
		return null;
	}

	public SchemaView getSchema(String schemaName) throws DalException, IOException, RestClientException {
		Schema schema = schemaDao.findLatestByName(schemaName, SchemaEntity.READSET_FULL);
		SchemaView schemaView = new SchemaView(schema);
		if (schema.getAvroid() > 0) {
			SchemaMetadata avroSchemaMeta = this.avroSchemaRegistry.getLatestSchemaMetadata(schema.getName());
			Map<String, Object> config = new HashMap<>();
			config.put("avro.schema", avroSchemaMeta.getSchema());
			config.put("avro.id", avroSchemaMeta.getId());
			config.put("avro.version", avroSchemaMeta.getVersion());
			schemaView.setConfig(config);
		}
		return schemaView;
	}

	public SchemaView updateSchema(SchemaView schemaView) throws DalException {
		Schema schema = schemaView.toMetaSchema();
		Schema oldSchema = schemaDao.findLatestByName(schema.getName(), SchemaEntity.READSET_FULL);
		schema.setVersion(oldSchema.getVersion() + 1);
		schema.setCreateTime(new Date(System.currentTimeMillis()));
		schema.setId(0);
		schemaDao.insert(schema);
		return new SchemaView(schema);
	}

	public void uploadJson(SchemaView schemaView, InputStream is, FormDataContentDisposition header) throws IOException,
	      DalException {
		byte[] fileBytes = ByteStreams.toByteArray(is);
		Schema metaSchema = schemaView.toMetaSchema();
		metaSchema.setFileContent(fileBytes);
		metaSchema.setFileProperties(JSON.toJSONString(header));
		schemaDao.updateByPK(metaSchema, SchemaEntity.UPDATESET_FULL);
	}

	public void uploadAvro(SchemaView schemaView, InputStream is, FormDataContentDisposition header) {
		// TODO Auto-generated method stub

	}

}
