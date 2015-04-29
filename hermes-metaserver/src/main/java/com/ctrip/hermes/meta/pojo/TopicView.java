package com.ctrip.hermes.meta.pojo;

import java.util.Date;
import java.util.List;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.entity.Codec;

public class TopicView {

	private Long id;

	private String name;

	private String storageType;

	private String description;

	private String status;

	private Date createTime;

	private Date lastModifiedTime;

	private List<Partition> partitions;

	private List<Property> properties;

	private Storage storage;

	private Long schemaId;
	
	private SchemaView schemaView;

	private String codecType;

	private Codec codec;

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
		this.partitions = topic.getPartitions();
		this.properties = topic.getProperties();
		this.codecType = topic.getCodecType();
		this.schemaId = topic.getSchemaId();
	}

	public Codec getCodec() {
	   return codec;
   }

	public String getCodecType() {
		return codecType;
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

	public Date getLastModifiedTime() {
		return lastModifiedTime;
	}

	public String getName() {
		return name;
	}

	public List<Partition> getPartitions() {
		return partitions;
	}

	public List<Property> getProperties() {
		return properties;
	}

	public SchemaView getSchema() {
		return schemaView;
	}

	public Long getSchemaId() {
	   return schemaId;
   }

	public String getStatus() {
		return status;
	}

	public Storage getStorage() {
		return storage;
	}

	public String getStorageType() {
		return storageType;
	}

	public void setCodec(Codec codec) {
	   this.codec = codec;
   }

	public void setCodecType(String codecType) {
		this.codecType = codecType;
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

	public void setLastModifiedTime(Date lastModifiedTime) {
		this.lastModifiedTime = lastModifiedTime;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setPartitions(List<Partition> partitions) {
		this.partitions = partitions;
	}

	public void setProperties(List<Property> properties) {
		this.properties = properties;
	}

	public void setSchema(SchemaView schemaView) {
		this.schemaView = schemaView;
	}

	public void setSchemaId(Long schemaId) {
	   this.schemaId = schemaId;
   }

	public void setStatus(String status) {
		this.status = status;
	}

	public void setStorage(Storage storage) {
		this.storage = storage;
	}

	public void setStorageType(String type) {
		this.storageType = type;
	}

	public Topic toMetaTopic() {
		Topic topic = new Topic();
		topic.setId(this.id);
		topic.setName(this.name);
		topic.setStorageType(this.storageType);
		topic.setDescription(this.description);
		if (this.properties != null) {
			for (Property prop : this.properties) {
				topic.addProperty(prop);
			}
		}
		if (this.partitions != null) {
			for (Partition partition : this.partitions) {
				topic.addPartition(partition);
			}
		}
		topic.setStatus(this.status);
		topic.setCreateTime(this.createTime);
		topic.setLastModifiedTime(this.lastModifiedTime);
		topic.setCodecType(this.codecType);
		topic.setSchemaId(this.schemaId);
		return topic;
	}
}
