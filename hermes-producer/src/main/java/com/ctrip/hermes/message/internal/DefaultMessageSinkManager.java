package com.ctrip.hermes.message.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.ProducerSinkManager;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.meta.entity.Connector;

public class DefaultMessageSinkManager extends ContainerHolder implements Initializable, ProducerSinkManager {

	@Inject
	private MetaService m_meta;

	private Map<String, PipelineSink> m_sinks = new HashMap<>();

	@Override
	public PipelineSink getSink(String topic) {
		PipelineSink sink = null;

		Connector connector = m_meta.getConnector(topic);
		switch (connector.getType()) {
		case Connector.BROKER:
		case Connector.TRANSACTION:
			sink = m_sinks.get(connector.getType());
			break;
		case Connector.LOCAL:
			sink = m_sinks.get(m_meta.getStorage(topic).getType());
			break;
		default:
			throw new RuntimeException(String.format("Unknown connector type %s of topic %s", connector.getType(), topic));
		}

		if (sink == null) {
			throw new RuntimeException(String.format("Unknown message sink for topic %s", topic));
		}

		return sink;
	}

	@Override
	public void initialize() throws InitializationException {
		Map<String, PipelineSink> sinks = lookupMap(PipelineSink.class);

		for (Entry<String, PipelineSink> entry : sinks.entrySet()) {
			m_sinks.put(entry.getKey(), entry.getValue());
		}
	}
}
