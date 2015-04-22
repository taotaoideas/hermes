package com.ctrip.hermes.meta.pojo;

import java.util.Date;
import java.util.List;

import com.ctrip.hermes.meta.dal.meta.Schema;
import com.ctrip.hermes.meta.entity.Property;

public class SchemaView {
	private Long id;

	private String name;

	private String type;

	private Integer version;

	private Date createTime;

	private String compatibility;

	private String description;

	private List<Property> properties;

	private String schemaPreview;

	public SchemaView() {

	}

	public SchemaView(Schema schema) {
		this.id = schema.getId();
		this.name = schema.getName();
		this.type = schema.getType();
		this.version = schema.getVersion();
		this.createTime = schema.getCreateTime();
		this.description = schema.getDescription();
		this.compatibility = schema.getCompatibility();
		if (schema.getSchemaContent() != null) {
			this.schemaPreview = new String(schema.getSchemaContent());
		}
	}

	public String getCompatibility() {
		return compatibility;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public String getDescription() {
		return description;
	}

	public Long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public List<Property> getProperties() {
		return properties;
	}

	public String getSchemaPreview() {
		return schemaPreview;
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

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setProperties(List<Property> properties) {
		this.properties = properties;
	}

	public void setSchemaPreview(String schemaPreview) {
		this.schemaPreview = schemaPreview;
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
		schema.setDescription(this.description);
		return schema;
	}

}
