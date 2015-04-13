package com.ctrip.hermes.meta.pojo;

import java.util.Date;
import java.util.Map;

import com.ctrip.hermes.meta.dal.meta.Schema;

public class SchemaView {
	private int id;

	private String name;

	private String type;

	private int version;

	private Date createTime;

	private Map<String, Object> config;
	
	public SchemaView() {

	}

	public SchemaView(Schema schema) {
		this.id = schema.getId();
		this.name = schema.getName();
		this.type = schema.getType();
		this.version = schema.getVersion();
		this.createTime = schema.getCreateTime();
	}

	public Date getCreateTime() {
		return createTime;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	public Integer getVersion() {
		return version;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	public Map<String, Object> getConfig() {
		return config;
	}

	public void setConfig(Map<String, Object> config) {
		this.config = config;
	}

	public Schema toMetaSchema() {
		Schema schema = new Schema();
		schema.setId(this.id);
		schema.setName(this.name);
		schema.setType(this.type);
		schema.setVersion(this.version);
		schema.setCreateTime(this.createTime);
		return schema;
	}

}
