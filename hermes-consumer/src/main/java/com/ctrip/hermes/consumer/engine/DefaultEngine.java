package com.ctrip.hermes.consumer.engine;

import java.util.List;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.engine.bootstrap.ConsumerBootstrap;
import com.ctrip.hermes.consumer.engine.bootstrap.ConsumerBootstrapManager;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Topic;

@Named(type = Engine.class)
public class DefaultEngine extends ContainerHolder implements Engine {

	@Inject
	private ConsumerBootstrapManager m_consumerManager;

	@Inject
	private MetaService m_metaService;

	@Override
	public void start(List<Subscriber> subscribers) {
		for (Subscriber s : subscribers) {
			List<Topic> topics = m_metaService.findTopicsByPattern(s.getTopicPattern());

			for (Topic topic : topics) {
				String endpointType = m_metaService.getEndpointType(topic.getName());
				ConsumerContext consumerContext = new ConsumerContext(topic, s.getGroupId(), s.getConsumer(),
				      s.getMessageClass());
				ConsumerBootstrap consumerBootstrap = m_consumerManager.findConsumerBootStrap(endpointType);
				consumerBootstrap.start(consumerContext);
			}
		}
	}

}
