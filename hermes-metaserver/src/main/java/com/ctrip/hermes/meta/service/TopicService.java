package com.ctrip.hermes.meta.service;

import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.dal.meta.MetaDao;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.pojo.TopicView;

@Named
public class TopicService {

	@Inject
	private MetaService m_meta;

	@Inject
	private MetaDao m_metaDao;
	
	public Topic getTopic(String topic) {
		return m_meta.findTopic(topic);
	}

	public List<Topic> findTopics(String pattern) {
		return m_meta.findTopicsByPattern(pattern);
	}

	public void createTopic(TopicView topic) {
	   // TODO Auto-generated method stub
	   
   }
}
