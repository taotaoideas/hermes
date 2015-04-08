package com.ctrip.hermes.meta.service;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Topic;

@Named
public class TopicService {

	@Inject
	private MetaService m_meta;

	public Topic getTopic(String topic) {
		return m_meta.findTopic(topic);
	}
}
