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
import com.ctrip.hermes.meta.Meta;
import com.ctrip.hermes.meta.MetaManager;

public class DefaultMessageSinkManager extends ContainerHolder implements Initializable, MessageSinkManager {

	@Inject
	private MetaManager m_metaManager;

	private Map<StorageType, MessageSink> m_sinks = new HashMap<StorageType, MessageSink>();

	@Override
	public MessageSink getSink(String topic) {
		Meta meta = m_metaManager.getMeta(topic);
		return m_sinks.get(meta.getStorageType(topic));
	}

	@Override
	public void initialize() throws InitializationException {
		Map<String, MessageSink> sinks = lookupMap(MessageSink.class);

		for (Entry<String, MessageSink> entry : sinks.entrySet()) {
			m_sinks.put(StorageType.valueOf(entry.getKey()), entry.getValue());
		}
	}
}
