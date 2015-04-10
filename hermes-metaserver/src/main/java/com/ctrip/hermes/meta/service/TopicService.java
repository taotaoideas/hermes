package com.ctrip.hermes.meta.service;

import java.util.Date;
import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.pojo.TopicView;

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

	public void createTopic(TopicView topicView) {
		Meta meta = m_metaManager.getMeta();
		Topic topic = topicView.toMetaTopic();
		topic.setCreateTime(new Date(System.currentTimeMillis()));
		// TODO topic ID, schema ID
		meta.addTopic(topic);
		m_metaManager.updateMeta(meta);
	}
}
