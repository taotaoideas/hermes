package com.ctrip.hermes.meta.service;

import java.util.Date;
import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Meta;
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
		topic.setId(maxTopicId);
		meta.addTopic(topic);
		m_metaManager.updateMeta(meta);
		return topic;
	}

	public Topic updateTopic(Topic topic) {
		Meta meta = m_metaManager.getMeta();
		meta.removeTopic(topic.getName());
		topic.setLastModifiedTime(new Date(System.currentTimeMillis()));
		meta.addTopic(topic);
		m_metaManager.updateMeta(meta);
		return topic;
	}

	public void deleteTopic(String name) {
		Meta meta = m_metaManager.getMeta();
		meta.removeTopic(name);
		m_metaManager.updateMeta(meta);
	}
}
