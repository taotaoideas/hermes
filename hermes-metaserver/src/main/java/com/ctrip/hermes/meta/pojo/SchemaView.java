package com.ctrip.hermes.meta.pojo;

import java.util.Date;

public class SchemaView {
	private String name;

	private Integer version;

	private Object schema;

	private Date createTime;

	public SchemaView() {

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
}
