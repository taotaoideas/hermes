package com.ctrip.hermes.meta.service;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.admin.AdminUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

@Named
public class TopicService {

	@Inject(ServerMetaManager.ID)
	private MetaManager m_metaManager;

	@Inject(ServerMetaService.ID)
	private MetaService m_metaService;

	public Topic getTopic(String topic) {
		return m_metaService.findTopic(topic);
	}

	public Topic getTopic(long topicId) {
		return m_metaService.findTopic(topicId);
	}

	public List<Topic> findTopics(String pattern) {
		return m_metaService.findTopicsByPattern(pattern);
	}

	public Storage findStorage(String topic) {
		return m_metaService.findStorage(topic);
	}

	public Topic createTopic(Topic topic) {
		Meta meta = m_metaManager.getMeta();
		topic.setCreateTime(new Date(System.currentTimeMillis()));
		long maxTopicId = 0;
		for (Topic topic2 : meta.getTopics().values()) {
			if (topic2.getId() != null && topic2.getId() > maxTopicId) {
				maxTopicId = topic2.getId();
			}
		}
		topic.setId(maxTopicId + 1);
		meta.addTopic(topic);

		m_metaManager.updateMeta(meta);
		m_metaService.refreshMeta(meta);
		return topic;
	}

	public Topic updateTopic(Topic topic) {
		Meta meta = m_metaManager.getMeta();
		meta.removeTopic(topic.getName());
		topic.setLastModifiedTime(new Date(System.currentTimeMillis()));
		meta.addTopic(topic);
		m_metaManager.updateMeta(meta);
		m_metaService.refreshMeta(meta);
		return topic;
	}

	public void deleteTopic(String name) {
		Meta meta = m_metaManager.getMeta();
		meta.removeTopic(name);
		m_metaManager.updateMeta(meta);
		m_metaService.refreshMeta(meta);
	}

	public void createTopicInKafka(Topic topic) {
		List<Partition> partitions = m_metaService.getPartitions(topic.getName());
		if (partitions == null || partitions.size() < 1) {
			return;
		}

		String consumerDatasource = partitions.get(0).getReadDatasource();
		Storage targetStorage = m_metaService.findStorage(topic.getName());
		if (targetStorage == null) {
			return;
		}

		String zkConnect = null;
		for (Datasource datasource : targetStorage.getDatasources()) {
			if (consumerDatasource.equals(datasource.getId())) {
				Map<String, Property> properties = datasource.getProperties();
				for (Map.Entry<String, Property> prop : properties.entrySet()) {
					if ("zookeeper.connect".equals(prop.getValue().getName())) {
						zkConnect = prop.getValue().getValue();
						break;
					}
				}
			}
		}

		ZkClient zkClient = new ZkClient(zkConnect);
		zkClient.setZkSerializer(new ZKStringSerializer());
		int partition = 1;
		int replication = 1;
		Properties topicProp = new Properties();
		for (Property prop : topic.getProperties()) {
			if ("replication-factor".equals(prop.getName())) {
				replication = Integer.parseInt(prop.getValue());
			} else if ("partitions".equals(prop.getName())) {
				partition = Integer.parseInt(prop.getValue());
			} else {
				topicProp.setProperty(prop.getName(), prop.getValue());
			}
		}
		AdminUtils.createTopic(zkClient, topic.getName(), partition, replication, topicProp);
	}
}

class ZKStringSerializer implements ZkSerializer {

	@Override
	public byte[] serialize(Object data) throws ZkMarshallingError {
		byte[] bytes = null;
		try {
			bytes = data.toString().getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new ZkMarshallingError(e);
		}
		return bytes;
	}

	@Override
	public Object deserialize(byte[] bytes) throws ZkMarshallingError {
		if (bytes == null)
			return null;
		else
			try {
				return new String(bytes, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new ZkMarshallingError(e);
			}
	}

}