package com.ctrip.hermes.consumer.engine.bootstrap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseConsumerBootstrap implements ConsumerBootstrap {

	@Inject
	protected EndpointChannelManager m_endpointChannelManager;

	@Inject
	protected EndpointManager m_endpointManager;

	@Inject
	protected MetaService m_metaService;
	
	@Inject
	protected ConsumerNotifier m_consumerNotifier;

	protected Map<Long, ConsumerContext> m_consumerContexts = new ConcurrentHashMap<>();

	public void start(ConsumerContext consumerContext) {
		doStart(consumerContext);
	}

	protected abstract void doStart(ConsumerContext consumerContext);
	
}
