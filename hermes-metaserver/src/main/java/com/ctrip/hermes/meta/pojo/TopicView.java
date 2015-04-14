package com.ctrip.hermes.meta.pojo;

import java.util.Date;
import java.util.List;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

public class TopicView {

	private Long id;

	private String name;

	private String storageType;

	private String description;

	private String status;

	private Date createTime;

	private Date lastModifiedTime;

	private long schemaId;

	private List<Partition> partitions;

	private List<Property> properties;

	private Storage storage;

	private SchemaView schemaView;

	private CodecView codecView;

	public TopicView() {

	}

	public TopicView(Topic topic) {
		this.id = topic.getId();
		this.name = topic.getName();
		this.storageType = topic.getStorageType();
		this.description = topic.getDescription();
		this.status = topic.getStatus();
		this.createTime = topic.getCreateTime();
		this.lastModifiedTime = topic.getLastModifiedTime();
		this.schemaId = topic.getSchemaId();
		this.partitions = topic.getPartitions();
		this.properties = topic.getProperties();

		this.codecView = new CodecView(topic.getCodec());
	}

	public Date getCreateTime() {
		return createTime;
	}

	public String getDescription() {
		return description;
	}

	public Date getLastModifiedTime() {
		return lastModifiedTime;
	}

	public String getName() {
		return name;
	}

	public long getSchemaId() {
		return schemaId;
	}

	public SchemaView getSchema() {
		return schemaView;
	}

	public String getStatus() {
		return status;
	}

	public String getStorageType() {
		return storageType;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public void setLastModifiedTime(Date lastModifiedTime) {
		this.lastModifiedTime = lastModifiedTime;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setSchemaId(int schemaId) {
		this.schemaId = schemaId;
	}

	public void setSchema(SchemaView schemaView) {
		this.schemaView = schemaView;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public void setStorageType(String type) {
		this.storageType = type;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public CodecView getCodec() {
		return codecView;
	}

	public void setCodec(CodecView codec) {
		this.codecView = codec;
	}

	public Topic toMetaTopic() {
		Topic topic = new Topic();
		topic.setId(this.id);
		topic.setName(this.name);
		topic.setStorageType(this.storageType);
		topic.setDescription(this.description);
		for (Property prop : this.properties) {
			topic.addProperty(prop);
		}
		topic.setStatus(this.status);
		topic.setCreateTime(this.createTime);
		topic.setLastModifiedTime(this.lastModifiedTime);
		topic.setSchemaId(this.schemaId);
		topic.setCodec(this.codecView.toMetaCodec());
		return topic;
	}

	public List<Partition> getPartitions() {
		return partitions;
	}

	public void setPartitions(List<Partition> partitions) {
		this.partitions = partitions;
	}

	public List<Property> getProperties() {
		return properties;
	}

	public void setProperties(List<Property> properties) {
		this.properties = properties;
	}

	public Storage getStorage() {
		return storage;
	}

	public void setStorage(Storage storage) {
		this.storage = storage;
	}
}
