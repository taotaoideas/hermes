package com.ctrip.hermes.message.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.MessageSink;
import com.ctrip.hermes.message.MessageSinkManager;
import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.meta.entity.Connector;

public class DefaultMessageSinkManager extends ContainerHolder implements Initializable, MessageSinkManager {

	@Inject
	private MetaService m_meta;

	private Map<String, MessageSink> m_sinks = new HashMap<String, MessageSink>();

	@Override
	public MessageSink getSink(String topic) {
		MessageSink sink = null;

		String connectorType = m_meta.getConnectorType(topic);
		switch (connectorType) {
		case Connector.BROKER:
		case Connector.TRANSACTION:
			sink = m_sinks.get(connectorType);
			break;
		case Connector.LOCAL:
			sink = m_sinks.get(m_meta.getStorageType(topic));
			break;
		default:
			throw new RuntimeException(String.format("Unknown connector type %s of topic %s", connectorType, topic));
		}

		if (sink == null) {
			throw new RuntimeException(String.format("Unknown message sink for topic %s", topic));
		}

		return sink;
	}

	@Override
	public void initialize() throws InitializationException {
		Map<String, MessageSink> sinks = lookupMap(MessageSink.class);

		for (Entry<String, MessageSink> entry : sinks.entrySet()) {
			m_sinks.put(entry.getKey(), entry.getValue());
		}
	}
}
