package com.ctrip.hermes.meta.pojo;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;

import java.util.Date;

public class SchemaView {
	private int id;

	private String name;

	private int version;

	private Object schema;

	private Date createTime;

	public SchemaView() {

	}

	public SchemaView(SchemaMetadata avroSchemaMeta) {
		this.id = avroSchemaMeta.getId();
		this.schema = avroSchemaMeta.getSchema();
		this.version = avroSchemaMeta.getVersion();
	}

	public Date getCreateTime() {
		return createTime;
	}

	public String getName() {
		return name;
	}

	public Object getSchema() {
		return schema;
	}

	public Integer getVersion() {
		return version;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setSchema(Object schema) {
		this.schema = schema;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
}
