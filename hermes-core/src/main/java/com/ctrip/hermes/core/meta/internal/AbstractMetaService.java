package com.ctrip.hermes.core.meta.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.transform.BaseVisitor2;

public abstract class AbstractMetaService implements MetaService, Initializable {

	protected Meta m_meta;

	protected Map<Long, Topic> m_topics;

	@Override
	public String getEndpointType(String topic) {
		if (m_meta.isDevMode()) {
			return Endpoint.LOCAL;
		} else {
			String endpointId = m_meta.getTopics().get(topic).getPartitions().get(0).getEndpoint();
			Endpoint endpoint = m_meta.getEndpoints().get(endpointId);
			if (endpoint == null) {
				throw new RuntimeException(String.format("Endpoint for topic %s is not found", topic));
			} else {
				return endpoint.getType();
			}
		}
	}

	@Override
	public List<Partition> getPartitions(String topicName) {
		Topic topic = m_meta.findTopic(topicName);
		if (topic != null) {
			return topic.getPartitions();
		}
		return null;
	}

	@Override
	public Endpoint findEndpoint(String endpointId) {
		return m_meta.findEndpoint(endpointId);
	}

	@Override
	public Storage findStorage(String topic) {
		String storageType = m_meta.findTopic(topic).getStorageType();
		return m_meta.findStorage(storageType);
	}

	@Override
	public Codec getCodecByTopic(String topicName) {
		Topic topic = m_meta.findTopic(topicName);
		if (topic != null) {
			String codeType = topic.getCodecType();
			return m_meta.getCodecs().get(codeType);
		} else
			return null;
	}

	@Override
	public Partition findPartition(String topicName, int partitionId) {
		Topic topic = m_meta.findTopic(topicName);
		if (topic != null)
			return topic.findPartition(partitionId);
		else
			return null;
	}

	public List<Topic> findTopicsByPattern(String topicPattern) {
		List<Topic> matchedTopics = new ArrayList<>();

		Collection<Topic> topics = m_meta.getTopics().values();

		Pattern pattern = Pattern.compile(topicPattern);

		for (Topic topic : topics) {
			if (pattern.matcher(topic.getName()).matches()) {
				matchedTopics.add(topic);
			}
		}

		return matchedTopics;
	}

	@Override
	public Topic findTopic(String topic) {
		return m_meta.findTopic(topic);
	}

	@Override
	public List<Partition> getPartitions(String topic, String groupId) {
		// TODO 对一个group的不同机器返回不一样的Partition
		return getPartitions(topic);
	}

	@Override
	public int getGroupIdInt(String groupId) {
		// TODO
		return 100;
	}

	@Override
	public List<Datasource> listMysqlDataSources() {
		final List<Datasource> dataSources = new ArrayList<>();

		m_meta.accept(new BaseVisitor2() {

			@Override
			protected void visitDatasourceChildren(Datasource ds) {
				Storage storage = getAncestor(2);

				if ("mysql".equalsIgnoreCase(storage.getType())) {
					dataSources.add(ds);
				}

				super.visitDatasourceChildren(ds);
			}

		});

		return dataSources;
	}

	// TODO add lock
	protected void refreshMeta(Meta meta) {
		m_meta = meta;
		m_topics = new HashMap<>();

		m_meta.accept(new BaseVisitor2() {

			@Override
			protected void visitTopicChildren(Topic topic) {
				m_topics.put(topic.getId(), topic);

				super.visitTopicChildren(topic);
			}

		});
	}

	@Override
	public Topic findTopic(long topicId) {
		return m_topics.get(topicId);
	}

	@Override
	public int getAckTimeoutSeconds(String topic) {
		// TODO
		return 2;
	}
	
	@Override
   public Codec getCodecByType(String codecType) {
	   return m_meta.findCodec(codecType);
   }
}
