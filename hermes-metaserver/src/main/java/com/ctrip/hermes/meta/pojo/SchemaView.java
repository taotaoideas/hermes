package com.ctrip.hermes.meta.pojo;

import java.util.Date;
import java.util.Map;

import com.ctrip.hermes.meta.dal.meta.Schema;

public class SchemaView {
	private Long id;

	private String name;

	private String type;

	private Integer version;

	private Date createTime;

	private String compatibility;

	private Map<String, Object> config;

	public SchemaView() {

	}

	public SchemaView(Schema schema) {
		this.id = schema.getId();
		this.name = schema.getName();
		this.type = schema.getType();
		this.version = schema.getVersion();
		this.createTime = schema.getCreateTime();
		this.compatibility = schema.getCompatibility();
	}

	public String getCompatibility() {
		return compatibility;
	}

	public Map<String, Object> getConfig() {
		return config;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public Long getId() {
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

	public void setCompatibility(String compatibility) {
		this.compatibility = compatibility;
	}

	public void setConfig(Map<String, Object> config) {
		this.config = config;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public Schema toMetaSchema() {
		Schema schema = new Schema();
		if (this.id != null) {
			schema.setId(this.id);
		}
		schema.setName(this.name);
		schema.setType(this.type);
		if (this.version != null) {
			schema.setVersion(this.version);
		}
		schema.setCreateTime(this.createTime);
		schema.setCompatibility(this.compatibility);
		return schema;
	}

}
